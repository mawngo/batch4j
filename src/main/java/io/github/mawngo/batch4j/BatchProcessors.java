package io.github.mawngo.batch4j;

import io.github.mawngo.batch4j.handlers.BatchHandler;
import io.github.mawngo.batch4j.impl.BatchingProcessorBuilder;
import io.github.mawngo.batch4j.impl.CallerRunBatchingProcessorBuilder;

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
}
