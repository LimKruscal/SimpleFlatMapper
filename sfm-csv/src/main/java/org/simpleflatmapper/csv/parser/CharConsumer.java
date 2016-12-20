package org.simpleflatmapper.csv.parser;


import java.io.IOException;

/**
 * Consume the charBuffer.
 */
public final class CharConsumer {


	public static final int LAST_CHAR_WAS_CR            = 64;
	public static final int END_OF_ROW = 32;
	public static final int COMMENTED                   = 16;
	public static final int ESCAPED_BOUNDARY            = 8;
	public static final int ESCAPED                     = 4;
	public static final int CELL_DATA                   = 2;
	public static final int LAST_CHAR_WAS_SEPARATOR     = 1;
	public static final int NONE                        = 0;

	private static final char LF = '\n';
	private static final char CR = '\r';
	private static final char SPACE = ' ';
	private static final char COMMENT = '#';

	private final CharBuffer csvBuffer;
	private final TextFormat textFormat;
	private final CellPreProcessor cellPreProcessor;

	private int _currentIndex = 0;
	private int _currentState = NONE;

	public CharConsumer(CharBuffer csvBuffer, TextFormat textFormat, CellPreProcessor cellPreProcessor) {
		this.csvBuffer = csvBuffer;
		this.cellPreProcessor = cellPreProcessor;
		this.textFormat = textFormat;
	}

	public final void consumeAllBuffer(final CellConsumer cellConsumer) {
		consume(cellConsumer, false);
	}


	public final boolean consumeToNextRow(CellConsumer cellConsumer) {
		return consume(cellConsumer, true);
	}


	private final boolean consume(final CellConsumer cellConsumer, final boolean stopAtEndOfRow) {

		final int bufferSize =  csvBuffer.bufferSize;
		final char[] buffer = csvBuffer.buffer;

		int currentState = _currentState;
		int currentIndex = _currentIndex;

		boolean trimSpaces = textFormat.trimSpaces;
		boolean yamlComment = textFormat.yamlComment;
		char escapeChar = textFormat.escapeChar;
		char separatorChar = textFormat.separatorChar;
		CellPreProcessor cellPreProcessor = this.cellPreProcessor;

		ml:
		while(currentIndex < bufferSize) {
			switch (currentState & (~(LAST_CHAR_WAS_SEPARATOR| LAST_CHAR_WAS_CR ))) {
				case NONE:
				{
					if (trimSpaces) {
						// skip spaces
						while(currentIndex < bufferSize) {
							if (buffer[currentIndex] != SPACE) {
								break;
							}
							currentIndex ++;
						}
						if (currentIndex >= bufferSize) {
							_currentIndex = bufferSize;
							_currentState = currentState;
							return false;
						}
					}

					csvBuffer.mark = currentIndex;
					char c = buffer[currentIndex];

					if (c == escapeChar) {
						currentIndex = currentIndex + 1;
						currentState = ESCAPED;
						continue ml;
					} else if (yamlComment
							&& currentState == NONE
							&& c == COMMENT) {
						currentIndex = currentIndex + 1;
						currentState = COMMENTED;
						continue ml;
					} // continue cell data
				}
				case CELL_DATA:
					for(int i = currentIndex; i < bufferSize; i++) {
						char c = buffer[i];
						if (c == separatorChar) {
							currentIndex = i + 1;
							cellPreProcessor.newCell(buffer, csvBuffer.mark, i, cellConsumer, CELL_DATA);
							currentState = LAST_CHAR_WAS_SEPARATOR;
							continue ml;
						} else if (c == LF) {
							currentIndex = i + 1;
							if (currentState != LAST_CHAR_WAS_CR) {
								cellPreProcessor.newCell(buffer, csvBuffer.mark, i, cellConsumer, CELL_DATA);
								currentState = END_OF_ROW;
							} else {
								csvBuffer.mark = currentIndex;
								currentState = NONE;
							}
							continue ml;
						} else if (c == CR) {
							currentIndex = i + 1;
							cellPreProcessor.newCell(buffer, csvBuffer.mark, i, cellConsumer, CELL_DATA);
							currentState = END_OF_ROW | LAST_CHAR_WAS_CR;
							continue ml;
						}
					}
					_currentIndex = bufferSize;
					_currentState = CELL_DATA;
					return false;
				case ESCAPED: {
					int i = findNexChar(buffer, currentIndex, bufferSize, textFormat.escapeChar);
					if (i != -1) {
						currentIndex = i + 1;
					} else {
						_currentIndex = bufferSize;
						_currentState = ESCAPED;
						return false;
					}
				}
				case ESCAPED_BOUNDARY:
					for(int i = currentIndex; i < bufferSize; i++) {
						char c = buffer[i];
						if (c == separatorChar) {
							currentIndex = i + 1;
							cellPreProcessor.newCell(buffer, csvBuffer.mark, i, cellConsumer, ESCAPED_BOUNDARY);
							currentState = LAST_CHAR_WAS_SEPARATOR;
							continue ml;
						} else if (c == LF) {
							currentIndex = i + 1;
							cellPreProcessor.newCell(buffer, csvBuffer.mark, i, cellConsumer, ESCAPED_BOUNDARY);
							currentState = END_OF_ROW;
							continue ml;
						} else if (c == CR) {
							currentIndex = i + 1;
							cellPreProcessor.newCell(buffer, csvBuffer.mark, i, cellConsumer, ESCAPED_BOUNDARY);
							currentState = END_OF_ROW | LAST_CHAR_WAS_CR;
							continue ml;
						} else  if (c == escapeChar) {
							currentIndex = i + 1;
							currentState = ESCAPED;
							continue ml;
						}
					}
					_currentIndex = bufferSize;
					_currentState = ESCAPED_BOUNDARY;
					return false;
				case COMMENTED:
					for(int i = currentIndex; i < bufferSize; i++) {
						char c = buffer[i];
						if (c == LF) {
							currentIndex = i + 1;
							cellPreProcessor.newCell(buffer, csvBuffer.mark, i, cellConsumer, COMMENTED);
							currentState = END_OF_ROW;
							continue ml;
						} else if (c == CR) {
							currentIndex = i + 1;
							cellPreProcessor.newCell(buffer, csvBuffer.mark, i, cellConsumer, COMMENTED);
							currentState = END_OF_ROW | LAST_CHAR_WAS_CR;
							continue ml;
						}
					}
					_currentIndex = bufferSize;
					_currentState = COMMENTED;
					return false;
				case END_OF_ROW:
					currentState = currentState & LAST_CHAR_WAS_CR;
					if (cellConsumer.endOfRow() && stopAtEndOfRow) {
						_currentIndex = currentIndex;
						_currentState = currentState;
						return true;
					}
			}
		}
		_currentIndex = currentIndex;
		_currentState = currentState;
		return false;
	}


	private int findNexChar(char[] chars, int start, int end, char c) {
		for(int i = start; i < end; i++) {
			if (chars[i] == c) return i;
		}
		return -1;
	}


	public final void finish(CellConsumer cellConsumer) {
		if (hasUnconsumedData(_currentState)) {
			cellPreProcessor.newCell(csvBuffer.buffer, csvBuffer.mark, _currentIndex, cellConsumer, _currentState);
			csvBuffer.mark = _currentIndex + 1;
			_currentState = NONE;
		} else if (_currentState == LAST_CHAR_WAS_SEPARATOR) {
			cellPreProcessor.newCell(csvBuffer.buffer, 0, 0, cellConsumer, _currentState);
			_currentState = NONE;
		}
		cellConsumer.end();
	}

	private static boolean hasUnconsumedData(int currentState) {
		return (currentState & (ESCAPED | ESCAPED_BOUNDARY | COMMENTED | CELL_DATA)) != 0;
	}

	public boolean next() throws IOException {
		int mark = csvBuffer.mark;
		boolean b = csvBuffer.next();
		int shift = mark - csvBuffer.mark;
		_currentIndex -= shift;
		return b;
	}


	public static boolean isEscaped(int state) {
		return (state & (ESCAPED | ESCAPED_BOUNDARY)) != 0;
	}

	public static boolean isCommented(int state) {
		return state == CharConsumer.COMMENTED;
	}
}
