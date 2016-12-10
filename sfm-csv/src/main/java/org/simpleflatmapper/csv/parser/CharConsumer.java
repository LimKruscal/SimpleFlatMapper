package org.simpleflatmapper.csv.parser;


import java.io.IOException;

/**
 * Consume the charBuffer.
 */
public final class CharConsumer {


	public static final int ROW_DATA                    = 64;
	public static final int COMMENTED                   = 32;
	public static final int CELL_DATA                   = 16;
	public static final int ESCAPED                     = 8;
	public static final int LAST_CHAR_WAS_SEPARATOR     = 4;
	public static final int LAST_CHAR_WAS_CR            = 2;
	public static final int ESCAPED_AREA                = 1;
	public static final int NONE                        = 0;

	private static final int TURN_OFF_LAST_CHAR_MASK = ~(LAST_CHAR_WAS_CR|LAST_CHAR_WAS_SEPARATOR);
	private static final int TURN_OFF_ESCAPED_AREA = ~(ESCAPED_AREA);

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

		final boolean notIgnoreLeadingSpace = !cellPreProcessor.ignoreLeadingSpace();
		final boolean yamlComment = textFormat.yamlComment;
		final char escapeChar = textFormat.escapeChar;
		final char separatorChar = textFormat.separatorChar;

		int currentState = _currentState;
		int currentIndex = _currentIndex;

		final char[] chars = csvBuffer.buffer;
		final int bufferSize =  csvBuffer.bufferSize;

		while(currentIndex < bufferSize) {
			// unescaped loop
			if ((currentState & ESCAPED_AREA) == 0) {
				if ((currentState & COMMENTED) == 0) {
					int lSize = Math.min(bufferSize, chars.length);
					while (currentIndex < lSize) {
						final char character = chars[currentIndex];
						final int cellEnd = currentIndex;

						currentIndex++;

						if (character == separatorChar) { // separator
							cellPreProcessor.newCell(chars, csvBuffer.mark, cellEnd, cellConsumer, currentState);
							csvBuffer.mark = currentIndex;
							currentState = LAST_CHAR_WAS_SEPARATOR | ROW_DATA;
							continue;
						} else if (character == LF) { // \n
							if ((currentState & LAST_CHAR_WAS_CR) == 0) {
								cellPreProcessor.newCell(chars, csvBuffer.mark, cellEnd, cellConsumer, currentState);
								cellConsumer.endOfRow();
							}
							csvBuffer.mark = currentIndex;
							currentState = NONE;
							continue;
						} else if (character == CR) { // \r
							cellPreProcessor.newCell(chars, csvBuffer.mark, cellEnd, cellConsumer, currentState);
							csvBuffer.mark = currentIndex;
							currentState = LAST_CHAR_WAS_CR;
							cellConsumer.endOfRow();
							continue;
						} else if (((currentState ^ CELL_DATA) & (ESCAPED | CELL_DATA)) != 0 && character == escapeChar) {
							currentState = ESCAPED_AREA | ESCAPED;
							break;
						} else if (yamlComment && (currentState & (CELL_DATA | ROW_DATA)) == 0 && character == COMMENT) {
							currentState |= COMMENTED;
							break;
						}

						currentState &= TURN_OFF_LAST_CHAR_MASK;
						if (notIgnoreLeadingSpace || character != SPACE) {
							currentState |= CELL_DATA;
						}
					}
				} else { // comment
					int nextEndOfLineChar = findNexEndOfLineChar(chars, currentIndex, bufferSize);
					if (nextEndOfLineChar != -1) {
						cellPreProcessor.newCell(chars, csvBuffer.mark, nextEndOfLineChar, cellConsumer, currentState);
						cellConsumer.endOfRow();
						currentIndex = nextEndOfLineChar + 1;
						csvBuffer.mark = currentIndex;
						currentState = chars[nextEndOfLineChar] == CR ? LAST_CHAR_WAS_CR : NONE;
					} else {
						currentIndex = bufferSize;
					}
				}
			} else {
				// escaped area
				int nextEscapeChar = findNexChar(chars, currentIndex, bufferSize, escapeChar);
				if (nextEscapeChar != -1) {
					currentIndex = nextEscapeChar + 1;
					currentState &= TURN_OFF_ESCAPED_AREA;
				} else {
					currentIndex = bufferSize;
				}
			}
		}

		_currentState = currentState;
		_currentIndex = currentIndex;
	}


	public final boolean consumeToNextRow(CellConsumer cellConsumer) {
		final boolean notIgnoreLeadingSpace = !cellPreProcessor.ignoreLeadingSpace();
		final char escapeChar = textFormat.escapeChar;
		final char separatorChar = textFormat.separatorChar;
		final boolean yamlComment = textFormat.yamlComment;

		int currentState = _currentState;
		int currentIndex = _currentIndex;

		final char[] chars = csvBuffer.buffer;
		final int bufferSize =  csvBuffer.bufferSize;

		while(currentIndex < bufferSize) {
			// unescaped loop
			if ((currentState & ESCAPED_AREA) == 0) {
				if ((currentState & COMMENTED) == 0) {
					while(currentIndex < bufferSize) {
						final char character = chars[currentIndex];
						final int cellEnd = currentIndex;

						currentIndex ++;

						if (character == separatorChar) { // separator
							cellPreProcessor.newCell(chars, csvBuffer.mark, cellEnd, cellConsumer, currentState);
							csvBuffer.mark = currentIndex;
							currentState = LAST_CHAR_WAS_SEPARATOR | ROW_DATA;
							continue;
						} else if (character == LF) { // \n
							if ((currentState & LAST_CHAR_WAS_CR) == 0) {
								cellPreProcessor.newCell(chars, csvBuffer.mark, cellEnd, cellConsumer, currentState);
								if (cellConsumer.endOfRow()) {
									csvBuffer.mark = currentIndex;
									_currentState = NONE;
									_currentIndex = currentIndex;
									return true;
								}
							}
							csvBuffer.mark = currentIndex;
							currentState = NONE;
							continue;
						} else if (character == CR) { // \r
							cellPreProcessor.newCell(chars, csvBuffer.mark, cellEnd, cellConsumer, currentState);
							csvBuffer.mark = currentIndex;
							currentState = LAST_CHAR_WAS_CR;
							if (cellConsumer.endOfRow()) {
								_currentState = currentState;
								_currentIndex = currentIndex;
								return true;
							}
							continue;
						} else if (((currentState ^ CELL_DATA) & (ESCAPED | CELL_DATA)) != 0 && character == escapeChar) {
							currentState = ESCAPED_AREA | ESCAPED;
							break;
						} else if (yamlComment && (currentState & (CELL_DATA | ROW_DATA)) == 0 && character == COMMENT) {
							currentState |= COMMENTED;
							break;
						}

						currentState &= TURN_OFF_LAST_CHAR_MASK;
						if (notIgnoreLeadingSpace || character != SPACE) {
							currentState |= CELL_DATA;
						}
					}
				} else {
					int nextEndOfLineChar = findNexEndOfLineChar(chars, currentIndex, bufferSize);
					if (nextEndOfLineChar != -1) {
						currentIndex = nextEndOfLineChar + 1;
						cellPreProcessor.newCell(chars, csvBuffer.mark, nextEndOfLineChar, cellConsumer, currentState);
						cellConsumer.endOfRow();
						csvBuffer.mark = currentIndex;
						currentState = chars[nextEndOfLineChar] == CR ? LAST_CHAR_WAS_CR : NONE;
					} else {
						currentIndex = bufferSize;
					}
				}
			} else {
				int nextEscapeChar = findNexChar(chars, currentIndex, bufferSize, escapeChar);
				if (nextEscapeChar != -1) {
					currentIndex = nextEscapeChar + 1;
					currentState &= TURN_OFF_ESCAPED_AREA;
				} else {
					currentIndex = bufferSize;
				}
			}
		}

		_currentState = currentState;
		_currentIndex = currentIndex;

		return false;
	}

	private int findNexChar(char[] chars, int start, int end, char c) {
		for(int i = start; i < end; i++) {
			if (chars[i] == c) return i;
		}
		return -1;
	}

	private int findNexEndOfLineChar(char[] chars, int start, int end) {
		for(int i = start; i < end; i++) {
			char c = chars[i];
			if (c == CR || c == LF) return i;
		}
		return -1;
	}

	public final void finish(CellConsumer cellConsumer) {
		if ( hasUnconsumedData()
				|| (_currentState & LAST_CHAR_WAS_SEPARATOR) != 0) {
			cellPreProcessor.newCell(csvBuffer.buffer, csvBuffer.mark, _currentIndex, cellConsumer, _currentState);
			csvBuffer.mark = _currentIndex + 1;
			_currentState = NONE;
		}
		cellConsumer.end();
	}

	private boolean hasUnconsumedData() {
		return _currentIndex > csvBuffer.mark;
	}

	public boolean next() throws IOException {
		int mark = csvBuffer.mark;
		boolean b = csvBuffer.next();
		_currentIndex -= mark - csvBuffer.mark;
		return b;
	}
}
