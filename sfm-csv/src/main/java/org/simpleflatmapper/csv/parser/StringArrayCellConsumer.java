package org.simpleflatmapper.csv.parser;

import org.simpleflatmapper.util.CheckedConsumer;
import org.simpleflatmapper.util.ErrorHelper;
import java.util.Arrays;

public final class StringArrayCellConsumer<RH extends CheckedConsumer<? super String[]>> implements CellConsumer {

	public static final int DEFAULT_MAX_NUMBER_OF_CELL_PER_ROW = 64 * 1024 * 1024;
	public static final int DEFAULT_ROW_SIZE = 8;
	private final RH handler;
	private final int maxNumberOfCellPerRow;
	private int currentIndex;
	private int currentLength = DEFAULT_ROW_SIZE;
	private String[] currentRow = new String[DEFAULT_ROW_SIZE];

	private StringArrayCellConsumer(RH handler, int maxNumberOfCellPerRow) {
		this.handler = handler;
		this.maxNumberOfCellPerRow = maxNumberOfCellPerRow;
	}

	@Override
	public void newCell(char[] chars, int offset, int length) {
		int currentIndex = this.currentIndex;
		ensureCapacity(currentIndex);
		currentRow[currentIndex] = new String(chars, offset, length);
		this.currentIndex++;
	}

	private void ensureCapacity(int currentIndex) {
		int currentLength = this.currentLength;
		if (currentIndex >= currentLength) {
			resize(currentIndex, currentLength);
		}
	}

	private void resize(int currentIndex, int currentLength) {
		int newLength = Math.min(currentLength * 2, maxNumberOfCellPerRow);
		if (currentIndex >= newLength) {
            throw new ArrayIndexOutOfBoundsException("Reach maximum number of cell per row " + currentIndex);
        }
		this.currentRow = Arrays.copyOf(currentRow, newLength);
		this.currentLength = newLength;
	}

	@Override
	public boolean endOfRow() {
		try {
			return _endOfRow();
		} catch (Exception e) { return ErrorHelper.<Boolean>rethrow(e);  }
	}

	private boolean _endOfRow() throws Exception {
		handler.accept(Arrays.copyOf(currentRow, currentIndex));
		currentIndex = 0;
		return true;
	}

	public RH handler() {
		return handler;
	}

	@Override
	public void end() {
		if (currentIndex > 0) {
			endOfRow();
		}
	}
	public static <RH extends CheckedConsumer<? super String[]>> StringArrayCellConsumer<RH> newInstance(RH handler, int maxNumberOfCellPerRow) {
		return new StringArrayCellConsumer<RH>(handler, maxNumberOfCellPerRow);
	}

	public static <RH extends CheckedConsumer<? super String[]>> StringArrayCellConsumer<RH> newInstance(RH handler) {
		return newInstance(handler, DEFAULT_MAX_NUMBER_OF_CELL_PER_ROW);
	}
}