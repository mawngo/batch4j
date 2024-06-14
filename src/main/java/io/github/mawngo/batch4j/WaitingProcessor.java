package io.github.mawngo.batch4j;

/**
 * Batch processor that in setup or stopped phase.
 *
 * @param <T> Type of item in batch
 */
public interface WaitingProcessor<T, P extends RunningProcessor<T>> {
    P run();
}
