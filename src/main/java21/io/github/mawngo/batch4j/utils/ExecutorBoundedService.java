package io.github.mawngo.batch4j.utils;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * Simple wrapper for {@link ExecutorService} that use {@link Semaphore} for limiting maximum number of threads.
 * <p>
 * Typically used for limiting number of virtual thread.
 */
public final class ExecutorBoundedService implements ExecutorService {
    private final ExecutorService delegate;

    private final Semaphore semaphore;

    private final boolean blockAtRun;

    private final int limit;

    public ExecutorBoundedService(ExecutorService delegate, int limit, boolean fair, boolean blockAtRun) {
        this.delegate = delegate;
        this.semaphore = new Semaphore(limit, fair);
        this.limit = limit;
        this.blockAtRun = blockAtRun;
    }

    public ExecutorBoundedService(ExecutorService delegate, int limit, boolean fair) {
        this(delegate, limit, fair, false);
    }

    public ExecutorBoundedService(ExecutorService delegate, int limit) {
        this(delegate, limit, true, false);
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        if (!blockAtRun) acquire(semaphore);

        return delegate.submit(() -> {
            if (blockAtRun) acquire(semaphore);
            try {
                return task.call();
            } finally {
                semaphore.release();
            }
        });
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        if (!blockAtRun) acquire(semaphore);

        return delegate.submit(() -> {
            if (blockAtRun) acquire(semaphore);
            try {
                task.run();
            } finally {
                semaphore.release();
            }
        }, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        if (!blockAtRun) acquire(semaphore);

        return delegate.submit(() -> {
            if (blockAtRun) acquire(semaphore);
            try {
                task.run();
            } finally {
                semaphore.release();
            }
        });
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return delegate.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return delegate.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    @Override
    public void execute(Runnable command) {
        if (!blockAtRun) acquire(semaphore);

        delegate.submit(() -> {
            if (blockAtRun) acquire(semaphore);
            try {
                command.run();
            } finally {
                semaphore.release();
            }
        });
    }

    public void waitUtilIdle() throws InterruptedException {
        semaphore.acquire(limit);
        semaphore.release(limit);
    }

    public void waitUtilIdleUninterruptibly() {
        semaphore.acquireUninterruptibly(limit);
        semaphore.release(limit);
    }

    public boolean tryWaitUtilIdle(long timeout, TimeUnit unit) throws InterruptedException {
        final var ok = semaphore.tryAcquire(limit, timeout, unit);
        if (ok) {
            semaphore.release(limit);
        }
        return ok;
    }

    private static void acquire(Semaphore semaphore) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new RejectedExecutionException(e);
        }
    }
}
