package io.github.mawngo.batch4j.handlers;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Handle batch error.
 */
@FunctionalInterface
public interface BatchErrorHandler<B> {

    /**
     * Handle a batch error.
     *
     * @param batch the batch to process
     * @param count the size of batch at the time processing
     * @param e     the error
     */
    void accept(B batch, int count, Throwable e);

    static <T> BatchErrorHandler<T> from(BiConsumer<T, Throwable> consumer) {
        return (item, count, e) -> consumer.accept(item, e);
    }

    static <T> BatchErrorHandler<T> from(Consumer<T> consumer) {
        return (item, count, e) -> consumer.accept(item);
    }

    /**
     * @return a {@link BatchErrorHandler} that just ignore the error.
     */
    static <T> BatchErrorHandler<T> ignore() {
        return (item, count, e) -> {
            // Do nothing.
        };
    }

    /**
     * @return a {@link BatchErrorHandler} that retry at max {@code maxRetries} times.
     */
    static <T> BatchErrorHandler<T> retryFrom(BatchHandler<T> handler, int maxRetries) {
        return (item, count, e) -> {
            for (int i = 0; i < maxRetries; i++) {
                try {
                    handler.accept(item, count);
                    return;
                } catch (Throwable ex) {
                    // continue
                }
            }
            Logging.error("Error processing batch of {} items after {} retries", count, maxRetries, e);
        };
    }

    /**
     * @return a {@link BatchErrorHandler} that log the error.
     */
    static <T> BatchErrorHandler<T> logging() {
        return (item, count, e) -> {
            Logging.error("Error processing batch of {} items", count, e);
        };
    }
}
