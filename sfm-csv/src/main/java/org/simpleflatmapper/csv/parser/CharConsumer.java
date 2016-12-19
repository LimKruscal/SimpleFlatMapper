package org.simpleflatmapper.csv.parser;


import java.io.IOException;

/**
 * Consume the charBuffer.
 */
public final class CharConsumer {


	public static final int LAST_CHAR_WAS_CR            = 128;
	public static final int END_OF_ROW_CR               = 64;
	public static final int END_ROW                     = 32;
	public static final int COMMENTED                   = 16;
	public static final int ESCAPED_BOUNDARY            = 8;
	public static final int ESCAPED                     = 4;
	public static final int CELL_DATA                   = 2;
	public static final int LAST_CHAR_WAS_SEPARATOR     = 1;
	public static final int NONE                        = 0;

	public static final int END_OF_ROW_MASK = ~(END_OF_ROW_CR | END_ROW | LAST_CHAR_WAS_CR);

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

		CharBuffer csvBuffer = this.csvBuffer;
		final int bufferSize =  csvBuffer.bufferSize;
		final char[] buffer = csvBuffer.buffer;

		int currentState = _currentState;

		// use state to exit
		// simplified unconsumer -> cell data/ escaped/comment.
		// simplified ROW_DATA > NOT NONE?
		while(_currentIndex < bufferSize) {
			switch (currentState) {
				case NONE:
				case LAST_CHAR_WAS_SEPARATOR:
					currentState = nextStartOfCell(currentState, buffer, _currentIndex, bufferSize, cellConsumer); // end row potential
					break;
				case CELL_DATA:
					currentState = nextCellData(currentState, buffer,  csvBuffer.mark, _currentIndex, bufferSize, cellConsumer); // end row potential
					break;
				case ESCAPED_BOUNDARY:
				case ESCAPED:
					currentState = nextEscaped(currentState, buffer,  csvBuffer.mark, _currentIndex, bufferSize, cellConsumer); // end row potential
					break;
				case LAST_CHAR_WAS_CR:
					currentState = nextLastCharWasCR(buffer, _currentIndex, bufferSize, cellConsumer); // end row potential
					break;
				case COMMENTED:
					currentState = nextComment(currentState, buffer,  csvBuffer.mark, _currentIndex, bufferSize,cellConsumer); // end row potential
					break;
				case END_ROW:
					currentState = NONE;
					break;
				case END_OF_ROW_CR:
					currentState = LAST_CHAR_WAS_CR;
					break;
			}
		}

		_currentState = currentState;
	}

	public final boolean consumeToNextRow(CellConsumer cellConsumer) {
		CharBuffer csvBuffer = this.csvBuffer;
		final int bufferSize =  csvBuffer.bufferSize;
		final char[] buffer = csvBuffer.buffer;


		int currentState = _currentState;

		while(_currentIndex < bufferSize) {

			switch (currentState) {
				case NONE:
				case LAST_CHAR_WAS_SEPARATOR:
					currentState = nextStartOfCell(currentState, buffer, _currentIndex, bufferSize, cellConsumer); // end row potential
					break;
				case CELL_DATA:
					currentState = nextCellData(currentState, buffer, csvBuffer.mark,  _currentIndex, bufferSize, cellConsumer); // end row potential
					break;
				case ESCAPED_BOUNDARY:
				case ESCAPED:
					currentState = nextEscaped(currentState, buffer,  csvBuffer.mark, _currentIndex, bufferSize, cellConsumer); // end row potential
					break;
				case LAST_CHAR_WAS_CR:
					currentState = nextLastCharWasCR(buffer, _currentIndex, bufferSize, cellConsumer); // end row potential
					break;
				case COMMENTED:
					currentState = nextComment(currentState, buffer,  csvBuffer.mark, _currentIndex, bufferSize, cellConsumer); // end row potential
					break;
				case END_ROW:
					_currentState = NONE;
					return true;
				case END_OF_ROW_CR:
					_currentState = LAST_CHAR_WAS_CR;
					return true;
			}
		}
		_currentState = currentState;
		return false;
	}

	private int nextCellData(int currentState, char[] buffer, int cellStart, int currentIndex, int bufferSize, CellConsumer cellConsumer) {
		char separatorChar = textFormat.separatorChar;
		for(int i = currentIndex; i < bufferSize; i++) {
			char c = buffer[i];
			if (c == separatorChar) {
				_currentIndex = i + 1;
				return endOfCellSeparator(currentState, buffer, cellStart, i, cellConsumer);
			} else if (c == LF) {
				_currentIndex = i + 1;
				return endOfCellLF(currentState, buffer, cellStart, i, cellConsumer);
			} else if (c == CR) {
				_currentIndex = i + 1;
				return endOfCellCR(currentState, buffer, cellStart, i, cellConsumer);
			}
		}
		_currentIndex = bufferSize;
		return currentState;
	}

	private int nextComment(int currentState, char[] buffer, int cellStart, int currentIndex, int bufferSize, CellConsumer cellConsumer) {
		for(int i = currentIndex; i < bufferSize; i++) {
			char c = buffer[i];
			if (c == LF) {
				_currentIndex = i + 1;
				endOfCellLF(currentState, buffer, cellStart, i, cellConsumer);
				return NONE;
			} else if (c == CR) {
				_currentIndex = i + 1;
				endOfCellCR(currentState, buffer, cellStart, i, cellConsumer);
				return LAST_CHAR_WAS_CR;
			}
		}
		_currentIndex = bufferSize;
		return currentState;
	}

	private int nextEscaped(int currentState, char[] buffer, int cellStart, int currentIndex, int bufferSize, CellConsumer cellConsumer) {

		char escapeChar = textFormat.escapeChar;
		if ((currentState & ESCAPED_BOUNDARY) == 0) {
			return nextEscapedArea(currentState, buffer, cellStart, currentIndex, bufferSize, cellConsumer, escapeChar);
		} else {
			return nextEscapedUnescapeArea(currentState, buffer,  cellStart, currentIndex, bufferSize, cellConsumer, escapeChar);
		}
	}

	private int nextEscapedUnescapeArea(int currentState, char[] buffer, int cellStart, int currentIndex, int bufferSize, CellConsumer cellConsumer, char escapeChar) {
		char separatorChar = textFormat.separatorChar;
		int index = currentIndex;
		do {
            char c = buffer[index];
            int cellEnd = index;
            index++;
            if (c == separatorChar) {
                _currentIndex = index;
                return endOfCellSeparator(currentState, buffer, cellStart, cellEnd, cellConsumer);
            } else if (c == LF) {
                _currentIndex = index;
                return endOfCellLF(currentState, buffer, cellStart, cellEnd, cellConsumer);
            } else if (c == CR) {
                _currentIndex = index;
                return endOfCellCR(currentState, buffer, cellStart, cellEnd, cellConsumer);
            } else  if (c == escapeChar) {
				_currentIndex = index;
				currentState = ESCAPED;
				if (index < bufferSize) {
					return nextEscapedArea(currentState, buffer, cellStart, index, bufferSize, cellConsumer, escapeChar);
				}
            }
        } while(index < bufferSize);
		_currentIndex = bufferSize;
		return currentState;
	}

	private int nextEscapedArea(int currentState, char[] buffer, int cellStart, int currentIndex, int bufferSize, CellConsumer cellConsumer, char escapeChar) {
		int i = findNexChar(buffer, currentIndex, bufferSize, escapeChar);

		if (i != -1) {
			int nextIndex = i + 1;
			_currentIndex = nextIndex;
			currentState = ESCAPED_BOUNDARY;
			if (nextIndex < bufferSize) {
				return nextEscapedUnescapeArea(currentState, buffer, cellStart, nextIndex, bufferSize, cellConsumer, escapeChar);
			}
		} else {
			_currentIndex = bufferSize;
		}
		return currentState;
	}


	private int endOfCellCR(int currentState, char[] buffer, int start, int end, CellConsumer cellConsumer) {
		cellPreProcessor.newCell(buffer, start, end, cellConsumer, currentState);
		return cellConsumer.endOfRow() ? END_OF_ROW_CR : LAST_CHAR_WAS_CR;
	}

	private int endOfCellLF(int currentState, char[] buffer, int start, int end, CellConsumer cellConsumer) {
		cellPreProcessor.newCell(buffer, start, end, cellConsumer, currentState);
		return cellConsumer.endOfRow() ? END_ROW : NONE;
	}

	private int endOfCellSeparator(int currentState, char[] buffer, int start, int end, CellConsumer cellConsumer) {
		cellPreProcessor.newCell(buffer, start, end, cellConsumer, currentState);
		return LAST_CHAR_WAS_SEPARATOR;
	}


	private int nextLastCharWasCR(char[] buffer, int currentIndex, int bufferSize, CellConsumer cellConsumer) {
		char c = buffer[currentIndex];

		if (c == LF) {
			currentIndex ++;
			_currentIndex = currentIndex;
		}

		if (currentIndex < bufferSize) {
			return nextStartOfCell(NONE, buffer, currentIndex, bufferSize, cellConsumer);
		}

		return NONE;
	}

	private int nextStartOfCell(int currentState, char[] buffer, int currentIndex, int bufferSize, CellConsumer cellConsumer) {

		if (textFormat.trimSpaces) {
			// skip spaces
			for(;currentIndex < bufferSize; currentIndex++) {
				if (buffer[currentIndex] != SPACE) {
					break;
				}
			}
			if (currentIndex >= bufferSize) {
				_currentIndex = currentIndex;
				csvBuffer.mark = currentIndex;
				return currentState;
			}
		}

		csvBuffer.mark = currentIndex;

		int nextIndex = currentIndex + 1;
		_currentIndex = nextIndex;


		char c = buffer[currentIndex];
		if (c == textFormat.separatorChar) {
			return endOfCellSeparator(currentState, buffer, currentIndex, currentIndex, cellConsumer);
		} else if (c == LF) {
			return endOfCellLF(currentState, buffer, currentIndex, currentIndex, cellConsumer);
		} else if (c == CR) {
			return endOfCellCR(currentState, buffer, currentIndex, currentIndex, cellConsumer);
		} else if (c == textFormat.escapeChar) {
			currentState = ESCAPED;
			if (nextIndex < bufferSize) {
				return nextEscapedArea(currentState, buffer, currentIndex, nextIndex, bufferSize, cellConsumer, textFormat.escapeChar);
			}
		} else if (textFormat.yamlComment
				&& c == COMMENT
				&& (currentState == NONE)) {
			currentState = COMMENTED;
			if (nextIndex < bufferSize) {
				return nextComment(currentState, buffer, currentIndex, nextIndex, bufferSize, cellConsumer);
			}
		} else {
			currentState = CELL_DATA;
			if (nextIndex < bufferSize) {
				return nextCellData(currentState, buffer, currentIndex, nextIndex, bufferSize, cellConsumer);
			}
		}
		return currentState;
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
