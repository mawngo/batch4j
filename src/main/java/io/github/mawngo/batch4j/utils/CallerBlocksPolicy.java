package io.github.mawngo.batch4j.utils;

import java.util.concurrent.*;

/**
 * A {@link RejectedExecutionHandler} that blocks the caller until the executor has room in its queue, or timeout passed
 */
public final class CallerBlocksPolicy implements RejectedExecutionHandler {
    private final long maxWait;
    private final TimeUnit unit;

    public CallerBlocksPolicy(final long maxWait, final TimeUnit unit) {
        this.maxWait = maxWait;
        this.unit = unit;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        if (!executor.isShutdown()) {
            try {
                final BlockingQueue<Runnable> queue = executor.getQueue();
                if (!queue.offer(r, this.maxWait, this.unit)) {
                    throw new RejectedExecutionException("Timeout offering task to queue");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RejectedExecutionException("Interrupted", e);
            }
        } else {
            throw new RejectedExecutionException("Executor is closed");
        }
    }
}
