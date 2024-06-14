package io.github.mawngo.batch4j;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Batch processor that in running state.
 *
 * @param <T> Type of item in batch
 */
public interface RunningProcessor<T> extends AutoCloseable {
    /**
     * Force process the remaining batch. This method may run and block on caller thread.
     * <p>
     * This method may run on caller thread.
     * <p>
     * While we're flushing the current batch, other thread <b>can still add element to new batch</b>.
     */
    void flush();

    /**
     * Force process utils the processor queue is empty.
     * <p>
     * This method may run on caller thread.
     * <p>
     * This method may block forever as other thread can still add element to new batch
     */
    default void drain() {
        while (size() > 0) {
            flush();
        }
    }

    /**
     * Force process utils the processor queue is empty, or time out passed.
     * <p>
     * This method may run on caller thread.
     */
    default void drain(long timeout, TimeUnit unit) {
        final long now = System.nanoTime();
        final long wait = unit.toNanos(timeout);
        while (size() > 0 && System.nanoTime() - now <= wait) {
            flush();
        }
    }

    /**
     * Returns the number of waiting elements (current item in batch) of this processor. If this processor current batch size is more than
     * {@code Integer.MAX_VALUE} elements, returns {@code Integer.MAX_VALUE}.
     *
     * @return the number of elements in this processor
     */
    int size();

    /**
     * Returns the approximate number of waiting elements.
     */
    int approximateSize();

    /**
     * Inserts the specified element into this processor, waiting if necessary for space to become available.
     *
     * @param e the element to add
     *
     * @throws InterruptedException       if interrupted while waiting
     * @throws ClassCastException         if the class of the specified element prevents it from being added to this queue
     * @throws NullPointerException       if the specified element is null
     * @throws IllegalArgumentException   if some property of the specified element prevents it from being added to this queue
     * @throws RejectedExecutionException if the processor is closed and cannot process the item
     */
    void put(T e) throws InterruptedException;


    /**
     * Inserts the specified element into this processor, waiting up to the specified wait time if necessary for space to become available.
     *
     * @param e       the element to add
     * @param timeout how long to wait before giving up, in units of {@code unit}
     * @param unit    a {@code TimeUnit} determining how to interpret the {@code timeout} parameter
     *
     * @return {@code true} if successful, or {@code false} if the specified waiting time elapses before space is available
     * @throws InterruptedException       if interrupted while waiting
     * @throws ClassCastException         if the class of the specified element prevents it from being added to this queue
     * @throws NullPointerException       if the specified element is null
     * @throws IllegalArgumentException   if some property of the specified element prevents it from being added to this queue
     * @throws RejectedExecutionException if the processor is closed and cannot process the item
     */
    boolean offer(T e, long timeout, TimeUnit unit) throws InterruptedException;

    @Override
    void close();
}
