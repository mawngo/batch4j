package io.github.mawngo.batch4j.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Spliterator.ORDERED;

/**
 * An iterator which returns batches of items taken from another iterator
 */
public final class BatchingIterator<T> implements Iterator<List<T>> {
    /**
     * Given a stream, convert it to a stream of batches no greater than the batchSize.
     *
     * @param originalStream to convert
     * @param batchSize      maximum size of a batch
     * @param <T>            type of items in the stream
     *
     * @return a stream of batches taken sequentially from the original stream
     */
    public static <T> Stream<List<T>> batchedStreamOf(Stream<T> originalStream, int batchSize) {
        return from(originalStream.iterator(), batchSize).stream();
    }

    public static <T> Stream<List<T>> batchedStreamOf(Stream<T> originalStream, int batchSize, int characteristics, boolean parallel) {
        return from(originalStream.iterator(), batchSize).stream(characteristics, parallel);
    }

    public static <T> BatchingIterator<T> from(Iterator<T> iterator, int batchSize) {
        return new BatchingIterator<>(iterator, batchSize);
    }

    public Stream<List<T>> stream() {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(this, ORDERED),
                false);
    }

    public Stream<List<T>> stream(int characteristics, boolean parallel) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(this, characteristics),
                parallel);
    }

    private final int batchSize;
    private List<T> currentBatch;
    private final Iterator<T> sourceIterator;

    private BatchingIterator(Iterator<T> sourceIterator, int batchSize) {
        this.batchSize = batchSize;
        this.sourceIterator = sourceIterator;
    }

    @Override
    public boolean hasNext() {
        prepareNextBatch();
        return currentBatch != null && !currentBatch.isEmpty();
    }

    @Override
    public List<T> next() {
        return currentBatch;
    }

    private void prepareNextBatch() {
        currentBatch = new ArrayList<>(batchSize);
        while (sourceIterator.hasNext() && currentBatch.size() < batchSize) {
            currentBatch.add(sourceIterator.next());
        }
    }
}
