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
import java.util.concurrent.locks.ReentrantLock;

/**
 * A processor that batching and running on caller threads.
 *
 * @param <T> the type of input item
 * @param <B> the type of merged item
 */
public final class CallerRunBatchingProcessor<T, B> implements RunningProcessor<T>, ParallelProcessor<T> {
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Whether to block putting item in when thread is processing current batch.
     */
    private final boolean blockWhileProcessing;

    /**
     * Maximum number of item this processor can hold.
     * <p>
     * Consider configuring at least either of {@code maxItem} or {@link #softMaxWait} to a normal value, otherwise this processor won't batch and
     * process item individually.
     * <p>
     * Set to {@code -1} for unlimited.
     */
    private final int maxItem;

    /**
     * Maximum time waiting for the batch to be fill, after this period, the batch will be processed anyway.
     * <p>
     * This maximum time is not guaranteed, as this processor run batch on caller thread, the limit only trigger when a thread put item into it.
     * <p>
     * Note that the batch won't be processed if empty even when waiting threshold passed.
     * <p>
     * Set to {@code -1} to wait forever.
     */
    private final long softMaxWait;

    /**
     * Executor service to delegate to.
     */
    @Nullable
    private final ExecutorService executorService;

    /**
     * Batch handling logic.
     */
    private final BatchHandler<B> handler;

    /**
     * Batch merging logic.
     */
    private final BatchMerger<T, B> merger;

    /**
     * Counter for total merged item, reset after processed
     */
    private int count;

    /**
     * Merged item, nullable. the handler won't run if this item is null
     */
    @Nullable
    private B batch;

    private long startTimeNanos = System.nanoTime();

    @Nullable
    private final BatchErrorHandler<B> errorHandler;

    CallerRunBatchingProcessor(
            BatchHandler<B> handler,
            BatchMerger<T, B> merger,
            @Nullable BatchErrorHandler<B> onErrorHandler,
            int maxItem,
            long maxWaitNanos,
            boolean blockWhileProcessing,
            @Nullable ExecutorService executorService) {
        this.blockWhileProcessing = blockWhileProcessing;
        this.maxItem = maxItem;
        this.softMaxWait = maxWaitNanos;
        this.handler = handler;
        this.merger = merger;
        this.executorService = executorService;
        this.errorHandler = onErrorHandler;
    }

    private boolean shouldSleep() {
        if (batch == null) {
            return true;
        }
        return count < maxItem && (softMaxWait < 0 || (System.nanoTime() - startTimeNanos) < softMaxWait);
    }

    @SuppressWarnings("LockNotBeforeTry")
    @Override
    public void flush() {
        lock.lock();
        final B item = this.batch;
        final int count = this.count;
        try {
            if (item == null) {
                return;
            }
            this.batch = null;
            this.count = 0;

            startTimeNanos = System.nanoTime();
        } finally {
            lock.unlock();
        }
        processItem(item, count);
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    private void processItem(final B item, final int counter) {
        if (item == null) {
            return;
        }

        if (executorService != null) {
            executorService.submit(() -> safeHandle(item, counter));
            return;
        }

        safeHandle(item, counter);
    }

    private void safeHandle(B item, int counter) {
        try {
            this.handler.accept(item, counter);
        } catch (Throwable e) {
            if (errorHandler != null) {
                errorHandler.accept(item, counter, e);
                return;
            }
            // Bubble up error to caller thread.
            throw e;
        }
    }

    private boolean isLimitNotReached() {
        return maxItem < 0 || count < (maxItem - 1);
    }

    private void doOfferAndReleaseLock(T e, ReentrantLock lock) {
        B item;
        int counter = 0;
        try {
            if (isLimitNotReached() && shouldSleep()) {
                enqueue(e);
                return;
            }

            enqueue(e);
            item = this.batch;
            counter = this.count;
            this.batch = null;
            this.count = 0;
            if (blockWhileProcessing) {
                processItem(item, counter);
            }
            startTimeNanos = System.nanoTime();
        } finally {
            lock.unlock();
        }

        if (blockWhileProcessing) {
            return;
        }

        final boolean isInterrupted = Thread.interrupted();
        processItem(item, counter);
        if (isInterrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void enqueue(T e) {
        this.batch = merger.apply(this.batch, e);
        count++;
    }

    @Override
    public void putInterruptibly(T e) throws InterruptedException {
        Objects.requireNonNull(e);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        doOfferAndReleaseLock(e, lock);
    }

    @Override
    public boolean offer(T e, long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(e);
        final ReentrantLock lock = this.lock;
        final boolean locked = lock.tryLock(timeout, unit);
        if (locked) {
            doOfferAndReleaseLock(e, lock);
        }
        return locked;
    }

    @Override
    public int size() {
        lock.lock();
        try {
            return count;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int approximateSize() {
        return count;
    }

    @Nullable
    @Override
    public ExecutorService getExecutor() {
        return executorService;
    }

    @Override
    public void closeExecutor() {
        final ExecutorService localExecutorService;
        lock.lock();
        try {
            localExecutorService = executorService;
        } finally {
            lock.unlock();
        }
        ExecutorUtils.close(localExecutorService);
    }

    @Override
    public boolean shutdownExecutor(long timeout, TimeUnit unit) {
        ExecutorService localExecutorService = null;
        try {
            if (lock.tryLock(timeout, unit)) {
                localExecutorService = executorService;
                lock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        return ExecutorUtils.shutdown(localExecutorService, timeout, unit);
    }

    @Override
    public void close() {
        // No-op
    }
}
