package io.github.mawngo.batch4j.handlers;

import io.github.mawngo.batch4j.annotations.Nullable;

import java.util.*;
import java.util.function.Supplier;

/**
 * Merge item into batch
 */
@FunctionalInterface
public interface BatchMerger<T, B> {
    int UNLIMITED = -1;

    /**
     * Merge a new item into an existing batch or initializing new batch.
     *
     * @param batch the existing batch or null when not initialized.
     * @param item  item to merge into batch.
     *
     * @return the initialized/merged batch.
     */
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
     * @return a {@link BatchMerger} that merges into a list.
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

    /**
     * @return a {@link BatchMerger} that merges into a list.
     */
    static <T> BatchMerger<T, List<T>> mergeToList(int size) {
        return (buffer, item) -> {
            if (buffer == null) {
                buffer = size > UNLIMITED ? new ArrayList<>(size) : new ArrayList<>();
            }
            buffer.add(item);
            return buffer;
        };
    }

    /**
     * @return a {@link BatchMerger} that merges into a list.
     */
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

    /**
     * @return a {@link BatchMerger} that merges into a set.
     */
    static <T> BatchMerger<T, Set<T>> mergeToSet(int size) {
        return (buffer, item) -> {
            if (buffer == null) {
                buffer = size > UNLIMITED ? new HashSet<>(MapUtils.newHashSet(size)) : new HashSet<>();
            }
            buffer.add(item);
            return buffer;
        };
    }

    /**
     * @return a {@link BatchMerger} that merges into a set.
     */
    static <T> BatchMerger<T, Set<T>> mergeToSet() {
        return mergeToSet(UNLIMITED);
    }
}
