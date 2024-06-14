package io.github.mawngo.batch4j.impl;

import io.github.mawngo.batch4j.ParallelProcessor;
import io.github.mawngo.batch4j.RunningProcessor;
import io.github.mawngo.batch4j.WaitingProcessor;
import io.github.mawngo.batch4j.annotations.Nullable;
import io.github.mawngo.batch4j.handlers.BatchErrorHandler;
import io.github.mawngo.batch4j.handlers.BatchHandler;
import io.github.mawngo.batch4j.handlers.BatchMerger;
import io.github.mawngo.batch4j.utils.ExecutorUtils;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A processor that receive and aggregate item into a batch, and process once the number of merged items is large enough or timeout passed
 *
 * @param <T> the type of input item
 * @param <B> the type of batch
 */
public final class BatchingProcessor<T, B> implements RunningProcessor<T>, ParallelProcessor<T>, WaitingProcessor<T, BatchingProcessor<T, B>> {
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Condition for {@link #newTaskSubmissionThread Task submission thread} to processing.
     */
    private final Condition condition = lock.newCondition();

    /**
     * Condition for awaiting put
     */
    private final Condition notFull = lock.newCondition();

    /**
     * Lock for processing item in {@link #newTaskSubmissionThread Task submission thread}. If this set to null,
     * {@link #newTaskSubmissionThread Task submission thread} will use {@link #lock} as processing lock.
     * <p>
     * The processing lock is required to prevent interruption while processing, which caused by {@link #close()}
     */
    private final ReentrantLock processingLock;

    /**
     * Maximum number of item this processor can hold before start execute.
     * <p>
     * Consider configuring at least either of {@code maxItem} or {@link #maxWait} to a normal value, otherwise this processor won't batch and process
     * item individually.
     * <p>
     * Set to {@code -1} to disable (always execute)
     */
    private final int maxItem;

    /**
     * Maximum time waiting for the batch to be fill, after this period, the batch will be processed anyway. Note that the batch won't be processed if
     * empty even when waiting threshold passed.
     * <p>
     * Set to {@code -1} to wait forever.
     */
    private final long maxWait;

    /**
     * Executor service to delegate to
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
     * Task submission thread factory.
     */
    private final ThreadFactory threadFactory;

    /**
     * Counter for total merged item, reset after processed
     */
    private int count;

    /**
     * Merged item, nullable. the handler won't run if this item is null
     */
    @Nullable
    private B batch;

    /**
     * {@link #newTaskSubmissionThread Task submission thread.}
     */
    private Thread thread;

    @Nullable
    private final BatchErrorHandler<B> errorHandler;

    BatchingProcessor(
            BatchHandler<B> handler,
            BatchMerger<T, B> merger,
            @Nullable BatchErrorHandler<B> onErrorHandler,
            int maxItem,
            long maxWaitNanos,
            boolean blockWhileProcessing,
            @Nullable ThreadFactory threadFactory,
            @Nullable ExecutorService executorService) {
        this.processingLock = blockWhileProcessing ? null : new ReentrantLock();
        this.maxItem = maxItem;
        this.maxWait = maxWaitNanos;
        this.handler = handler;
        this.merger = merger;
        this.executorService = executorService;
        this.threadFactory = threadFactory != null ? threadFactory : Executors.defaultThreadFactory();
        this.errorHandler = onErrorHandler;
    }

    private Thread newTaskSubmissionThread() {
        return threadFactory.newThread(() -> {
            while (!isInterrupted()) {
                long startTime = System.nanoTime();
                B item;
                int counter = 0;
                lock.lock();
                try {
                    if (awaitAndCheckIsInterrupted(startTime)) {
                        return;
                    }

                    counter = this.count;
                    item = this.batch;
                    this.batch = null;
                    this.count = 0;
                    notFull.signalAll();
                    if (isLockWhileProcessing()) {
                        processItem(item, counter);
                    }
                } finally {
                    lock.unlock();
                }

                if (isLockWhileProcessing()) {
                    continue;
                }

                processingLock.lock();
                try {
                    final boolean isInterrupted = Thread.interrupted();
                    processItem(item, counter);
                    if (isInterrupted) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                } finally {
                    processingLock.unlock();
                }
            }
        });
    }

    private boolean isLockWhileProcessing() {
        return processingLock == null;
    }

    /**
     * Await loop to wait for {@link #shouldSleep(long)} specific condition. This method is intended to use in
     * {@link #newTaskSubmissionThread TaskSubmissionThread}
     *
     * @return whether the thread is interrupted, so we can decide to exit the {@link #newTaskSubmissionThread TaskSubmissionThread}
     */
    private boolean awaitAndCheckIsInterrupted(long startTime) {
        while (shouldSleep(startTime)) {
            if (isInterrupted()) return true;
            try {
                await();
                if (isInterrupted()) return true;
            } catch (InterruptedException e) {
                return true;
            }
        }
        return false;
    }

    /**
     * Make current thread await. This method is intended to use in {@link #newTaskSubmissionThread TaskSubmissionThread}
     */
    @SuppressWarnings("WaitNotInLoop")
    private void await() throws InterruptedException {
        if (maxWait > -1) {
            condition.awaitNanos(maxWait);
            return;
        }
        condition.await();
    }

    /**
     * Whether this thread should continue to sleep. This method is intended to use in {@link #newTaskSubmissionThread TaskSubmissionThread}
     *
     * @param startTimeNanos the start of waiting time.
     */
    private boolean shouldSleep(long startTimeNanos) {
        if (batch == null) {
            return true;
        }
        return count < maxItem && (maxWait < 0 || (System.nanoTime() - startTimeNanos) < maxWait);
    }

    /**
     * Check if current thread interrupted. This method is intended to use in {@link #newTaskSubmissionThread TaskSubmissionThread}
     */
    private boolean isInterrupted() {
        return Thread.currentThread().isInterrupted();
    }

    @SuppressWarnings("LockNotBeforeTry")
    @Override
    public void close() {
        lock.lock();
        if (!isLockWhileProcessing()) {
            processingLock.lock();
        }
        final Thread thread = this.thread;
        try {
            if (thread == null) return;
            thread.interrupt();
            this.thread = null;
        } finally {
            condition.signal();
            if (!isLockWhileProcessing()) {
                processingLock.unlock();
            }
            lock.unlock();
        }

        // This implementation make sure to process all the leftover item.
        while (true) {
            lock.lock();
            final B item = this.batch;
            final int count = this.count;
            try {
                // If thread is not null meaning that the processor is restarted, we can stop flushing
                if (this.thread != null) {
                    break;
                }
                if (item == null) {
                    break;
                }
                this.batch = null;
                this.count = 0;
                notFull.signalAll();
            } finally {
                lock.unlock();
            }
            processItem(item, count);
        }

        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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

            notFull.signalAll();
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
            if (this.errorHandler != null) {
                errorHandler.accept(item, count, e);
                return;
            }
            BatchErrorHandler.logging().accept(item, count, e);
        }
    }

    private void enqueue(T e) {
        this.batch = merger.apply(this.batch, e);
        count++;
        condition.signal();
    }

    @Override
    public void put(T e) throws InterruptedException {
        Objects.requireNonNull(e);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();

        if (thread == null) {
            throw new RejectedExecutionException("The processor is already closed");
        }

        try {
            while (isReachedLimit())
                notFull.await();
            enqueue(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(T e, long timeout, TimeUnit unit)
            throws InterruptedException {
        Objects.requireNonNull(e);
        long nanos = unit.toNanos(timeout);

        final ReentrantLock lock = this.lock;
        if (lock.tryLock() || lock.tryLock(timeout, unit)) {
            if (thread == null) {
                throw new RejectedExecutionException("The processor is already closed");
            }

            try {
                while (isReachedLimit()) {
                    if (nanos <= 0L)
                        return false;
                    nanos = notFull.awaitNanos(nanos);
                }
                enqueue(e);
                return true;
            } finally {
                lock.unlock();
            }
        }
        return false;
    }

    private boolean isReachedLimit() {
        return maxItem > -1 && count >= maxItem;
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
    public BatchingProcessor<T, B> run() {
        lock.lock();
        try {
            if (thread != null) return this;
            thread = newTaskSubmissionThread();
            thread.start();
        } finally {
            lock.unlock();
        }
        return this;
    }
}
