package io.github.mawngo.batch4j.impl;

import io.github.mawngo.batch4j.ParallelProcessor;
import io.github.mawngo.batch4j.RunningProcessor;
import io.github.mawngo.batch4j.annotations.Nullable;
import io.github.mawngo.batch4j.handlers.BatchErrorHandler;
import io.github.mawngo.batch4j.handlers.BatchHandler;
import io.github.mawngo.batch4j.handlers.BatchMerger;
import io.github.mawngo.batch4j.utils.ExecutorUtils;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A processor that run directly on callers thread without batching.
 */
public class DisabledBatchingProcessor<T> implements RunningProcessor<T>, ParallelProcessor<T> {
    /**
     * Executor service to delegate to
     */
    @Nullable
    private final ExecutorService executorService;

    private final Consumer<T> handler;

    protected <B> DisabledBatchingProcessor(
            BatchHandler<B> handler,
            BatchMerger<T, B> merger,
            BatchErrorHandler<B> errorHandler,
            ExecutorService executorService) {
        this((T item) -> {
            final var batch = merger.apply(null, item);
            try {
                handler.accept(batch, 1);
            } catch (Throwable e) {
                if (errorHandler != null) {
                    errorHandler.accept(batch, 1, e);
                    return;
                }
                BatchErrorHandler.logging().accept(batch, 1, e);
            }
        }, executorService);
    }

    protected DisabledBatchingProcessor(
            Consumer<T> handler,
            ExecutorService executorService) {
        this.handler = handler;
        this.executorService = executorService;
    }

    @Override
    public ExecutorService getExecutor() {
        return executorService;
    }

    @Override
    public void closeExecutor() {
        ExecutorUtils.close(executorService);
    }

    @Override
    public boolean shutdownExecutor(long timeout, TimeUnit unit) {
        return ExecutorUtils.shutdown(executorService, timeout, unit);
    }

    @Override
    public void flush() {
        // No-op
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    @Override
    public boolean offer(T item) {
        Objects.requireNonNull(item);
        if (executorService != null) {
            executorService.submit(() -> this.handler.accept(item));
            return true;
        }

        this.handler.accept(item);
        return true;
    }

    @Override
    public void put(T e) {
        offer(e);
    }

    @Override
    public boolean offer(T e, long timeout, TimeUnit unit) {
        return offer(e);
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public int approximateSize() {
        return 0;
    }

    @Override
    public void close() throws Exception {
        // No-op
    }
}
