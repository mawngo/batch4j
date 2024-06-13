package io.github.mawngo.batch4j.handlers;

final class MapUtils {
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private MapUtils() {
    }

    public static int calculateHashMapCapacity(int size) {
        return (int) Math.ceil(size / (double) DEFAULT_LOAD_FACTOR);
    }
}
