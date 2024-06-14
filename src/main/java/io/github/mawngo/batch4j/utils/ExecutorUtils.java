package io.github.mawngo.batch4j.utils;

import io.github.mawngo.batch4j.annotations.Nullable;

import java.util.concurrent.*;

/**
 * Utilities to creating and managing {@link ExecutorService}
 */
public final class ExecutorUtils {
    private ExecutorUtils() {
    }

    /**
     * Shutdown the ExecutorService.
     */
    public static boolean shutdown(@Nullable ExecutorService executorService, long timeout, TimeUnit unit) {
        if (executorService == null) {
            return true;
        }
        if (executorService.isTerminated()) {
            return true;
        }
        try {
            executorService.shutdown();
            if (!executorService.awaitTermination(timeout, unit)) {
                executorService.shutdownNow();
                return false;
            }
            return true;
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Initiates an orderly shutdown in which previously submitted tasks are executed, but no new tasks will be accepted. This method waits until all
     * tasks have completed execution and the executor has terminated.
     *
     * <p> If interrupted while waiting, this method stops all executing tasks as
     * if by invoking {@link ExecutorService#shutdownNow()}. It then continues to wait until all actively executing tasks have completed. Tasks that
     * were awaiting execution are not executed. The interrupt status will be re-asserted before this method returns.
     *
     * <p> If already terminated, invoking this method has no effect.
     *
     * @throws SecurityException if a security manager exists and shutting down this ExecutorService may manipulate threads that the caller is not
     *                           permitted to modify because it does not hold {@link java.lang.RuntimePermission}{@code ("modifyThread")}, or the
     *                           security manager's {@code checkAccess} method denies access.
     * @since 19
     */
    public static void close(@Nullable ExecutorService executorService) {
        if (executorService == null) {
            return;
        }
        boolean terminated = executorService.isTerminated();
        if (!terminated) {
            executorService.shutdown();
            boolean interrupted = false;
            while (!terminated) {
                try {
                    terminated = executorService.awaitTermination(1L, TimeUnit.DAYS);
                } catch (InterruptedException e) {
                    if (!interrupted) {
                        executorService.shutdownNow();
                        interrupted = true;
                    }
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static ThreadPoolSize parseConcurrency(String concurrency) {
        return ThreadPoolSize.parse(concurrency);
    }

    /**
     * Create a new {@link ExecutorService} which it's characteristic depend on {@link ThreadPoolSize}
     *
     * @param threadPoolSize the core and max size of pool
     *
     * @return new ExecutorService
     */
    @Nullable
    public static ExecutorService newThreadPool(ThreadPoolSize threadPoolSize, ThreadFactory threadFactory) {
        if (threadPoolSize.isDisabled()) {
            return null;
        }
        if (threadPoolSize.isCached()) {
            return Executors.newCachedThreadPool();
        }
        if (threadPoolSize.isSingle()) {
            return Executors.newSingleThreadExecutor();
        }
        if (threadPoolSize.isFixed()) {
            return Executors.newFixedThreadPool(threadPoolSize.corePoolSize());
        }
        if (threadPoolSize.isDynamic()) {
            return newDynamicThreadPool(threadPoolSize.corePoolSize(), threadPoolSize.maximumPoolSize(), threadFactory);
        }
        throw new IllegalArgumentException("Invalid pool size " + threadPoolSize);
    }

    /**
     * Create a new {@link ExecutorService} that backed by {@link SynchronousQueue}  which it's characteristic depend on {@link ThreadPoolSize}
     *
     * @param threadPoolSize the core and max size of pool.
     *
     * @return new ExecutorService
     */
    @Nullable
    public static ExecutorService newSynchronousThreadPool(ThreadPoolSize threadPoolSize, ThreadFactory threadFactory) {
        if (threadPoolSize.isDisabled()) {
            return null;
        }
        if (threadPoolSize.isCached()) {
            return Executors.newCachedThreadPool();
        }
        if (threadPoolSize.isSingle()) {
            return new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    new SynchronousQueue<>(),
                    threadFactory);
        }
        if (threadPoolSize.isFixed()) {
            return new ThreadPoolExecutor(threadPoolSize.corePoolSize(), threadPoolSize.maximumPoolSize(),
                    0L, TimeUnit.MILLISECONDS,
                    new SynchronousQueue<>(),
                    threadFactory);
        }
        if (threadPoolSize.isDynamic()) {
            return new ThreadPoolExecutor(threadPoolSize.corePoolSize(), threadPoolSize.maximumPoolSize(),
                    60L, TimeUnit.SECONDS,
                    new SynchronousQueue<>(),
                    threadFactory);
        }
        throw new IllegalArgumentException("Invalid pool size " + threadPoolSize);
    }

    @Nullable
    public static ExecutorService newThreadPool(ThreadPoolSize threadPoolSize) {
        return newThreadPool(threadPoolSize, Executors.defaultThreadFactory());
    }

    @Nullable
    public static ExecutorService newThreadPool(String concurrency, ThreadFactory threadFactory) {
        return newThreadPool(parseConcurrency(concurrency), threadFactory);
    }

    @Nullable
    public static ExecutorService newThreadPool(String concurrency) {
        return newThreadPool(parseConcurrency(concurrency));
    }

    @Nullable
    public static ExecutorService newCallerBlocksThreadPool(ThreadPoolSize threadPoolSize, long maxWait, TimeUnit timeUnit) {
        final ExecutorService pool = newSynchronousThreadPool(threadPoolSize, Executors.defaultThreadFactory());
        if (pool instanceof ThreadPoolExecutor) {
            ((ThreadPoolExecutor) pool).setRejectedExecutionHandler(new CallerBlocksPolicy(maxWait, timeUnit));
        }
        return pool;
    }

    @Nullable
    public static ExecutorService newCallerBlocksThreadPool(ThreadPoolSize threadPoolSize) {
        return newCallerBlocksThreadPool(threadPoolSize, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Nullable
    public static ExecutorService newCallerBlocksThreadPool(String concurrency, long maxWait, TimeUnit timeUnit) {
        return newCallerBlocksThreadPool(parseConcurrency(concurrency), maxWait, timeUnit);
    }

    @Nullable
    public static ExecutorService newCallerBlocksThreadPool(String concurrency) {
        return newCallerBlocksThreadPool(parseConcurrency(concurrency));
    }

    /**
     * Creates a thread pool that reuses a fixed number of {@code corePoolSize} threads operating off a shared unbounded queue.  At any point, at most
     * {@code maxPoolSize} threads will be active processing tasks. If additional tasks are submitted when all threads are active, they will wait in
     * the queue until a thread is available. If any thread terminates due to a failure during execution prior to shutdown, a new one will take its
     * place if needed to execute subsequent tasks.  The threads in the pool will exist until it is explicitly
     * {@link ExecutorService#shutdown shutdown}.
     *
     * @param corePoolSize the minimum number of threads in the pool
     * @param maxPoolSize  the maximum number of threads in the pool
     *
     * @return the newly created thread pool
     * @throws IllegalArgumentException if {@code nThreads <= 0}
     */
    public static ExecutorService newDynamicThreadPool(int corePoolSize, int maxPoolSize, ThreadFactory threadFactory) {
        return new ThreadPoolExecutor(corePoolSize, maxPoolSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                threadFactory);
    }

    public static ExecutorService newDynamicThreadPool(int corePoolSize, int maxPoolSize) {
        return newDynamicThreadPool(corePoolSize, maxPoolSize, Executors.defaultThreadFactory());
    }

    /**
     * Creates a thread pool that creates new threads as needed, but will reuse previously constructed threads when they are available.  These pools
     * will typically improve the performance of programs that execute many short-lived asynchronous tasks. Calls to {@code execute} will reuse
     * previously constructed threads if available. If no existing thread is available, a new thread will be created and added to the pool. Threads
     * that have not been used for sixty seconds are terminated and removed from the cache. Thus, a pool that remains idle for long enough will not
     * consume any resources. Note that pools with similar properties but different details (for example, timeout parameters) may be created using
     * {@link ThreadPoolExecutor} constructors.
     *
     * @param corePoolSize the minimum number of threads to keep in the pool
     *
     * @return the newly created thread pool
     */
    public static ExecutorService newDynamicThreadPool(int corePoolSize, ThreadFactory threadFactory) {
        return new ThreadPoolExecutor(corePoolSize, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                threadFactory);
    }

    public static ExecutorService newDynamicThreadPool(int corePoolSize) {
        return newDynamicThreadPool(corePoolSize, Executors.defaultThreadFactory());
    }

    public static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }
}
