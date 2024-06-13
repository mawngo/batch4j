package io.github.mawngo.batch4j.handlers;

import java.util.HashSet;
import java.util.Set;

final class MapUtils {
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private MapUtils() {
    }

    private static int calculateHashMapCapacity(int size) {
        return (int) Math.ceil(size / (double) DEFAULT_LOAD_FACTOR);
    }

    public static <T> Set<T> newHashSet(int size) {
        return new HashSet<>(size);
    }
}
