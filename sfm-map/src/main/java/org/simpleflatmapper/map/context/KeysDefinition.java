package org.simpleflatmapper.map.context;

import org.simpleflatmapper.util.ErrorHelper;

import java.util.List;

public class KeysDefinition<S, K> {
    private final KeySourceGetter<K, S> keySourceGetter;
    private final List<K> keys;
    private final int parentIndex;
    private final int currentIndex;

    public KeysDefinition(List<K> keys, KeySourceGetter<K, S> keySourceGetter, int currentIndex, int parentIndex) {
        this.keys = keys;
        this.keySourceGetter = keySourceGetter;
        this.currentIndex = currentIndex;
        this.parentIndex = parentIndex;
    }

    public boolean isEmpty() {
        return keys.isEmpty();
    }

    public Object[] getValues(S source) {
        try {
            Object[] values = new Object[keys.size()];
            for (int i = 0; i < values.length; i++) {
                values[i] = keySourceGetter.getValue(keys.get(i), source);
            }
            return values;
        } catch (Exception e) {
            return ErrorHelper.rethrow(e);
        }
    }

    public int getParentIndex() {
        return parentIndex;
    }

    public int getIndex() {
        return currentIndex;
    }
}
