package io.github.mawngo.batch4j.handlers;

import java.util.HashSet;
import java.util.Set;

final class MapUtils {
    private MapUtils() {
    }

    public static <T> Set<T> newHashSet(int size) {
        return HashSet.newHashSet(size);
    }
}
