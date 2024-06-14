package io.github.mawngo.batch4j.impl;

import io.github.mawngo.batch4j.ParallelProcessor;
import io.github.mawngo.batch4j.RunningProcessor;
import io.github.mawngo.batch4j.WaitingProcessor;
import io.github.mawngo.batch4j.handlers.BatchErrorHandler;
import io.github.mawngo.batch4j.handlers.BatchHandler;
import io.github.mawngo.batch4j.handlers.BatchMerger;
import io.github.mawngo.batch4j.utils.ExecutorUtils;
import io.github.mawngo.batch4j.utils.ThreadPoolSize;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public final class BatchingProcessorBuilder<B> {
    public static final int UNLIMITED = -1;

    private final BatchHandler<B> handler;
    private BatchErrorHandler<B> errorHandler;
    private boolean blockWhileProcessing;
    private int maxItem = UNLIMITED;
    private long maxWaiNanos = UNLIMITED;
    private boolean disable;
    private ThreadFactory threadFactory;

    public BatchingProcessorBuilder(BatchHandler<B> batchHandler) {
        this.handler = batchHandler;
    }

    /**
     * Enable block while processing
     *
     * @see BatchingProcessor#processingLock
     */
    public BatchingProcessorBuilder<B> blockWhileProcessing() {
        this.blockWhileProcessing = true;
        return this;
    }

    /**
     * Configure the task submission thread factory.
     *
     * @see BatchingProcessor#threadFactory
     */
    public BatchingProcessorBuilder<B> threadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
        return this;
    }

    /**
     * Configure the maximum item this processor can hold.
     *
     * @see BatchingProcessor#maxItem
     */
    public BatchingProcessorBuilder<B> maxItem(int limit) {
        this.maxItem = limit;
        return this;
    }

    /**
     * Configure the maximum wait time before process.
     *
     * @see BatchingProcessor#maxWait
     */
    public BatchingProcessorBuilder<B> maxWait(long waitingTime, TimeUnit unit) {
        this.maxWaiNanos = unit.toNanos(waitingTime);
        return this;
    }

    /**
     * Configure {@link BatchErrorHandler} callback.
     *
     * @see BatchingProcessor#errorHandler
     */
    public BatchingProcessorBuilder<B> onError(BatchErrorHandler<B> errorHandler) {
        this.errorHandler = errorHandler;
        return this;
    }

    /**
     * Configure whether to disable this processor.
     */
    public BatchingProcessorBuilder<B> disableIf(boolean disable) {
        this.disable = disable;
        return this;
    }

    /**
     * Build the processor. If the {@link #disableIf processor is disabled}, it will return {@link DisabledBatchingProcessor}.
     */
    public <T> WaitingProcessor<T, RunningProcessor<T>> build(BatchMerger<T, B> merger) {
        if (!disable) {
            return () -> new DisabledBatchingProcessor<T>(handler, merger, errorHandler, null);
        }
        return () -> new BatchingProcessor<>(handler, merger, errorHandler, maxItem, maxWaiNanos, blockWhileProcessing, threadFactory, null);
    }

    /**
     * Delegate processing to the {@link ExecutorService}. It is recommended to use an {@code ExecutorService} with
     * {@link io.github.mawngo.batch4j.utils.CallerBlocksPolicy}
     *
     * @return {@link ParallelBuilder}
     * @see ExecutorUtils#newCallerBlocksThreadPool(String)
     */
    public ParallelBuilder<B> parallelTo(ExecutorService executorService) {
        return new ParallelBuilder<>(executorService, this);
    }

    /**
     * Create and delegate processing to the {@link ExecutorService}.
     *
     * @see ExecutorUtils#newCallerBlocksThreadPool(String)
     */
    public ParallelBuilder<B> parallel(String concurrency) {
        return parallelTo(ExecutorUtils.newCallerBlocksThreadPool(concurrency));
    }

    /**
     * Create and delegate processing to the {@link ExecutorService}.
     *
     * @see ExecutorUtils#newCallerBlocksThreadPool(ThreadPoolSize)
     */
    public ParallelBuilder<B> parallel(ThreadPoolSize threadPoolSize) {
        return parallelTo(ExecutorUtils.newCallerBlocksThreadPool(threadPoolSize));
    }

    /**
     * {@code CallerRunBatchingProcessorBuilder} for {@link ParallelProcessor}
     */
    public static final class ParallelBuilder<B> {
        private final ExecutorService executorService;
        private final BatchingProcessorBuilder<B> builder;

        private ParallelBuilder(ExecutorService executorService, BatchingProcessorBuilder<B> builder) {
            this.executorService = executorService;
            this.builder = builder;
        }

        /**
         * Build the processor. If the {@link #disableIf processor is disabled}, it will return {@link DisabledBatchingProcessor}.
         */
        public <T> WaitingProcessor<T, ParallelProcessor<T>> build(BatchMerger<T, B> merger) {
            if (!builder.disable) {
                return () -> new DisabledBatchingProcessor<T>(builder.handler, merger, builder.errorHandler, executorService);
            }
            return () -> new BatchingProcessor<>(builder.handler,
                    merger, builder.errorHandler, builder.maxItem, builder.maxWaiNanos,
                    builder.blockWhileProcessing, builder.threadFactory, executorService);
        }
    }
}
