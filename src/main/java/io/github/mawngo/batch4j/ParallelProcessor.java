package io.github.mawngo.batch4j;

import io.github.mawngo.batch4j.annotations.Nullable;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * {@link RunningProcessor} that delegate work to an {@link ExecutorService}
 *
 * @param <T> Type of item in batch
 */
public interface ParallelProcessor<T> extends RunningProcessor<T> {
    /**
     * Get the backing executors in case parallel is enabled.
     */
    @Nullable
    ExecutorService getExecutor();

    /**
     * Close the backing {@link ExecutorService}. This method run/block on caller thread.
     * <p>
     * This method must only be run after the processor is closed.
     * <p>
     * This method is not closing the processor and should be called after {@link #close()} if this processor is closable. You can close the
     * ExecutorService manually without using this method.
     */
    void closeExecutor();

    /**
     * Shutdown the backing {@link ExecutorService}. This method run/block on caller thread.
     * <p>
     * This method must only be run after the processor is closed.
     */
    boolean shutdownExecutor(long timeout, TimeUnit unit);
}
