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
import java.util.concurrent.TimeUnit;

public final class CallerRunBatchingProcessorBuilder<B> {
    public static final int UNLIMITED = -1;

    private final BatchHandler<B> handler;
    private BatchErrorHandler<B> errorHandler;
    private boolean blockWhileProcessing;
    private int maxItem = UNLIMITED;
    private long maxWaiNanos = UNLIMITED;
    private boolean disable;

    public CallerRunBatchingProcessorBuilder(BatchHandler<B> batchHandler) {
        this.handler = batchHandler;
    }

    /**
     * Enable block while processing
     *
     * @see CallerRunBatchingProcessor#blockWhileProcessing
     */
    public CallerRunBatchingProcessorBuilder<B> blockWhileProcessing() {
        this.blockWhileProcessing = true;
        return this;
    }

    /**
     * Configure the maximum item this processor can hold.
     *
     * @see CallerRunBatchingProcessor#maxItem
     */
    public CallerRunBatchingProcessorBuilder<B> maxItem(int limit) {
        this.maxItem = limit;
        return this;
    }

    /**
     * Configure the maximum wait time before process.
     *
     * @see CallerRunBatchingProcessor#softMaxWait
     */
    public CallerRunBatchingProcessorBuilder<B> maxWait(long waitingTime, TimeUnit unit) {
        this.maxWaiNanos = unit.toNanos(waitingTime);
        return this;
    }

    /**
     * Configure {@link BatchErrorHandler} callback.
     *
     * @see CallerRunBatchingProcessor#errorHandler
     */
    public CallerRunBatchingProcessorBuilder<B> onError(BatchErrorHandler<B> errorHandler) {
        this.errorHandler = errorHandler;
        return this;
    }

    /**
     * Configure whether to disable this processor.
     */
    public CallerRunBatchingProcessorBuilder<B> disableIf(boolean disable) {
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
        return () -> new CallerRunBatchingProcessor<>(handler, merger, errorHandler, maxItem, maxWaiNanos, blockWhileProcessing, null);
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
        private final CallerRunBatchingProcessorBuilder<B> builder;

        private ParallelBuilder(ExecutorService executorService, CallerRunBatchingProcessorBuilder<B> builder) {
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
            return () -> new CallerRunBatchingProcessor<>(builder.handler,
                    merger, builder.errorHandler, builder.maxItem, builder.maxWaiNanos,
                    builder.blockWhileProcessing, executorService);
        }
    }
}
