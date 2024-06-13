package io.github.mawngo.batch4j.handlers;

import io.github.mawngo.batch4j.annotations.Nullable;

import java.util.*;
import java.util.function.Supplier;

@FunctionalInterface
public interface BatchMerger<T, B> {
    int UNLIMITED = -1;

    B apply(@Nullable B batch, T item);

    /**
     * Create a {@link BatchMerger} that merges into a collection.
     */
    static <T> BatchMerger<T, Collection<T>> mergeToCollection(Supplier<Collection<T>> factory) {
        return (buffer, item) -> {
            if (buffer == null) {
                buffer = factory.get();
            }
            buffer.add(item);
            return buffer;
        };
    }

    /**
     * Create a {@link BatchMerger} that merges into a list.
     */
    static <T> BatchMerger<T, List<T>> mergeToList(Supplier<List<T>> factory) {
        return (buffer, item) -> {
            if (buffer == null) {
                buffer = factory.get();
            }
            buffer.add(item);
            return buffer;
        };
    }

    static <T> BatchMerger<T, List<T>> mergeToList(int size) {
        return (buffer, item) -> {
            if (buffer == null) {
                buffer = size > UNLIMITED ? new ArrayList<>(size) : new ArrayList<>();
            }
            buffer.add(item);
            return buffer;
        };
    }

    static <T> BatchMerger<T, List<T>> mergeToList() {
        return mergeToList(UNLIMITED);
    }

    /**
     * Create a {@link BatchMerger} that merges into a set.
     */
    static <T> BatchMerger<T, Set<T>> mergeToSet(Supplier<Set<T>> factory) {
        return (buffer, item) -> {
            if (buffer == null) {
                buffer = factory.get();
            }
            buffer.add(item);
            return buffer;
        };
    }

    static <T> BatchMerger<T, Set<T>> mergeToSet(int size) {
        return (buffer, item) -> {
            if (buffer == null) {
                buffer = size > UNLIMITED ? new HashSet<>(MapUtils.calculateHashMapCapacity(size)) : new HashSet<>();
            }
            buffer.add(item);
            return buffer;
        };
    }

    static <T> BatchMerger<T, Set<T>> mergeToSet() {
        return mergeToSet(UNLIMITED);
    }
}
