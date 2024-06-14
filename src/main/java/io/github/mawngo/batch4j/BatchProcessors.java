package io.github.mawngo.batch4j;

import io.github.mawngo.batch4j.handlers.BatchHandler;
import io.github.mawngo.batch4j.impl.BatchingProcessorBuilder;
import io.github.mawngo.batch4j.impl.CallerRunBatchingProcessorBuilder;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Builder factories and utilities for BatchProcessor.
 */
public final class BatchProcessors {
    private BatchProcessors() {
    }

    /**
     * @see BatchingProcessorBuilder
     */
    public static <B> BatchingProcessorBuilder<B> newBuilder(BatchHandler<B> batchHandler) {
        return new BatchingProcessorBuilder<>(batchHandler);
    }

    /**
     * @see BatchingProcessorBuilder
     */
    public static <B> BatchingProcessorBuilder<B> newBuilder(Consumer<B> batchHandler) {
        return new BatchingProcessorBuilder<>(BatchHandler.from(batchHandler));
    }

    /**
     * @see CallerRunBatchingProcessorBuilder
     */
    public static <B> CallerRunBatchingProcessorBuilder<B> newCallerRunBuilder(BatchHandler<B> batchHandler) {
        return new CallerRunBatchingProcessorBuilder<>(batchHandler);
    }

    /**
     * @see CallerRunBatchingProcessorBuilder
     */
    public static <B> CallerRunBatchingProcessorBuilder<B> newCallerRunBuilder(Consumer<B> batchHandler) {
        return new CallerRunBatchingProcessorBuilder<>(BatchHandler.from(batchHandler));
    }

    /**
     * Put item to processor, handle interruption exception by restore the interrupt.
     *
     * @see RunningProcessor#put(Object)
     */
    public static <T> void put(RunningProcessor<T> processor, T item) {
        try {
            processor.put(item);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Offer item to processor, handle interruption exception by restore the interrupt.
     *
     * @see RunningProcessor#offer(Object, long, TimeUnit)
     */
    public static <T> void offer(RunningProcessor<T> processor, T item, long timeout, TimeUnit unit) {
        try {
            processor.offer(item, timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
