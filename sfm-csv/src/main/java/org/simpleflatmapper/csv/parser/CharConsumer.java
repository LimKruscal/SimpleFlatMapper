package org.simpleflatmapper.csv.parser;


import java.io.IOException;

/**
 * Consume the charBuffer.
 */
public final class CharConsumer {


	public static final int END_OF_BUFFER               = 256;
	public static final int LAST_CHAR_WAS_CR            = 128;
	public static final int END_OF_ROW_CR               = 64;
	public static final int END_ROW                     = 32;
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

		CharBuffer csvBuffer = this.csvBuffer;
		final int bufferSize =  csvBuffer.bufferSize;
		final char[] buffer = csvBuffer.buffer;

		int currentState = _currentState;
		boolean trimSpaces = textFormat.trimSpaces;
		boolean yamlComment = textFormat.yamlComment;
		char escapeChar = textFormat.escapeChar;
		char separatorChar = textFormat.separatorChar;

		while(_currentIndex < bufferSize) {
			if ((currentState & (~LAST_CHAR_WAS_SEPARATOR)) == 0) {
				currentState = nextStartOfCell(trimSpaces, yamlComment, escapeChar, currentState, buffer, _currentIndex, bufferSize); // end row potential
			} else if (currentState == CELL_DATA) {
				currentState = nextCellData(separatorChar, buffer, _currentIndex, bufferSize, cellConsumer); // end row potential
			} else if (currentState == ESCAPED) {
				currentState = nextEscapedArea(buffer, _currentIndex, bufferSize); // end row potential
			} else if (currentState == ESCAPED_BOUNDARY) {
				currentState = nextEscapedUnescapeArea(buffer, _currentIndex, bufferSize, cellConsumer); // end row potential
			} else if (currentState == LAST_CHAR_WAS_CR) {
				currentState = nextLastCharWasCR(buffer, _currentIndex); // end row potential
			} else if (currentState == COMMENTED) {
				currentState = nextComment(buffer, _currentIndex, bufferSize, cellConsumer); // end row potential
			}

			currentState = (currentState & (~(END_ROW | END_OF_ROW_CR))) | ((currentState & END_OF_ROW_CR) << 1) ;
		}

		_currentState = currentState;
	}

	public final boolean consumeToNextRow(CellConsumer cellConsumer) {
		CharBuffer csvBuffer = this.csvBuffer;
		final int bufferSize =  csvBuffer.bufferSize;
		final char[] buffer = csvBuffer.buffer;


		int currentState = _currentState;
		boolean trimSpaces = textFormat.trimSpaces;
		boolean yamlComment = textFormat.yamlComment;
		char escapeChar = textFormat.escapeChar;
		char separatorChar = textFormat.separatorChar;

		while (_currentIndex < bufferSize) {
			if ((currentState & (~LAST_CHAR_WAS_SEPARATOR)) == 0) {
				currentState = nextStartOfCell(trimSpaces,yamlComment, escapeChar, currentState, buffer, _currentIndex, bufferSize); // end row potential
			} else if (currentState == CELL_DATA) {
				currentState = nextCellData(separatorChar, buffer, _currentIndex, bufferSize, cellConsumer); // end row potential
			} else if (currentState  == ESCAPED) {
				currentState = nextEscapedArea(buffer, _currentIndex, bufferSize); // end row potential
			} else if (currentState == ESCAPED_BOUNDARY) {
				currentState = nextEscapedUnescapeArea(buffer, _currentIndex, bufferSize, cellConsumer); // end row potential
			} else if (currentState == LAST_CHAR_WAS_CR) {
				currentState = nextLastCharWasCR(buffer, _currentIndex); // end row potential
			} else if (currentState == COMMENTED) {
				currentState = nextComment(buffer, _currentIndex, bufferSize, cellConsumer); // end row potential
			}

			if ((currentState & (END_ROW|END_OF_ROW_CR)) != 0) {
				_currentState = (currentState & (~(END_ROW | END_OF_ROW_CR))) | ((currentState & END_OF_ROW_CR) << 1) ;
				return true;
			}
		}
		_currentState = currentState;
		return false;
	}

	private int nextStartOfCell(boolean trimSpaces, boolean yamlComment, char escapeChar, int currentState,
								char[] buffer, int currentIndex, int bufferSize) {
		if (trimSpaces) {
			// skip spaces
			int i = findFirstNonSpace(buffer, currentIndex, bufferSize);
			if (i != -1) {
				currentIndex = i;
			} else {
				_currentIndex = bufferSize;
				return currentState;
			}
		}

		csvBuffer.mark = currentIndex;
		char c = buffer[currentIndex];

		if (c != escapeChar) {
			if (!yamlComment
					|| currentState != NONE
					|| c != COMMENT) {
				return CELL_DATA;
			} else {
				_currentIndex = currentIndex + 1;
				return COMMENTED;
			}
		} else {
			_currentIndex = currentIndex + 1;
			return ESCAPED;
		}
	}

	private int nextCellData(char separatorChar, char[] buffer, int currentIndex, int bufferSize, CellConsumer cellConsumer) {
		for(int i = currentIndex; i < bufferSize; i++) {
			char c = buffer[i];
			if (c == separatorChar) {
				_currentIndex = i + 1;
				cellPreProcessor.newCell(buffer, csvBuffer.mark, i, cellConsumer, CELL_DATA);
				return LAST_CHAR_WAS_SEPARATOR;
			} else if (c == LF) {
				_currentIndex = i + 1;
				cellPreProcessor.newCell(buffer, csvBuffer.mark, i, cellConsumer, CELL_DATA);
				return cellConsumer.endOfRow() ? END_ROW : NONE;
			} else if (c == CR) {
				_currentIndex = i + 1;
				cellPreProcessor.newCell(buffer, csvBuffer.mark, i, cellConsumer, CELL_DATA);
				return cellConsumer.endOfRow() ? END_OF_ROW_CR : LAST_CHAR_WAS_CR;
			}
		}
		_currentIndex = bufferSize;
		return CELL_DATA;
	}

	private int nextComment(char[] buffer, int currentIndex, int bufferSize, CellConsumer cellConsumer) {
		for(int i = currentIndex; i < bufferSize; i++) {
			char c = buffer[i];
			if (c == LF) {
				_currentIndex = i + 1;
				endOfCellLF(COMMENTED, buffer, i, cellConsumer);
				return NONE;
			} else if (c == CR) {
				_currentIndex = i + 1;
				endOfCellCR(COMMENTED, buffer, i, cellConsumer);
				return LAST_CHAR_WAS_CR;
			}
		}
		_currentIndex = bufferSize;
		return COMMENTED;
	}

	private int nextEscapedUnescapeArea(char[] buffer,  int currentIndex, int bufferSize, CellConsumer cellConsumer) {
		char separatorChar = textFormat.separatorChar;
		char escapeChar = textFormat.escapeChar;
		int index = currentIndex;
		do {
            char c = buffer[index];
            int cellEnd = index;
            index++;
            if (c == separatorChar) {
                _currentIndex = index;
                return endOfCellSeparator(ESCAPED_BOUNDARY, buffer, cellEnd, cellConsumer);
            } else if (c == LF) {
                _currentIndex = index;
                return endOfCellLF(ESCAPED_BOUNDARY, buffer, cellEnd, cellConsumer);
            } else if (c == CR) {
                _currentIndex = index;
                return endOfCellCR(ESCAPED_BOUNDARY, buffer, cellEnd, cellConsumer);
            } else  if (c == escapeChar) {
				_currentIndex = index;
				return ESCAPED;
            }
        } while(index < bufferSize);
		_currentIndex = bufferSize;
		return ESCAPED_BOUNDARY;
	}

	private int nextEscapedArea(char[] buffer, int currentIndex, int bufferSize) {
		int i = findNexChar(buffer, currentIndex, bufferSize, textFormat.escapeChar);

		if (i != -1) {
			_currentIndex = i + 1;
			return ESCAPED_BOUNDARY;
		} else {
			_currentIndex = bufferSize;
			return ESCAPED;
		}
	}


	private int endOfCellCR(int currentState, char[] buffer, int end, CellConsumer cellConsumer) {
		cellPreProcessor.newCell(buffer, csvBuffer.mark, end, cellConsumer, currentState);
		return cellConsumer.endOfRow() ? END_OF_ROW_CR : LAST_CHAR_WAS_CR;
	}

	private int endOfCellLF(int currentState, char[] buffer, int end, CellConsumer cellConsumer) {
		cellPreProcessor.newCell(buffer, csvBuffer.mark, end, cellConsumer, currentState);
		return cellConsumer.endOfRow() ? END_ROW : NONE;
	}

	private int endOfCellSeparator(int currentState, char[] buffer,  int end, CellConsumer cellConsumer) {
		cellPreProcessor.newCell(buffer, csvBuffer.mark, end, cellConsumer, currentState);
		return LAST_CHAR_WAS_SEPARATOR;
	}


	private int nextLastCharWasCR(char[] buffer, int currentIndex) {
		char c = buffer[currentIndex];
		if (c == LF) {
			_currentIndex = currentIndex + 1;
		}
		return NONE;
	}

	private int findFirstNonSpace(char[] buffer, int start, int end) {
		for(int i = start; i < end; i++) {
			if (buffer[i] != SPACE) return i;
		}
		return -1;
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
