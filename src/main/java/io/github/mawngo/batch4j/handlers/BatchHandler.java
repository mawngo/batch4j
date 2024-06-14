package io.github.mawngo.batch4j.handlers;

import java.util.function.Consumer;

/**
 * Process a batch.
 */
@FunctionalInterface
public interface BatchHandler<B> {
    /**
     * Process a batch.
     *
     * @param batch the batch to process
     * @param count the size of batch at the time processing
     */
    void accept(B batch, int count);

    static <T> BatchHandler<T> from(Consumer<T> consumer) {
        return (item, count) -> consumer.accept(item);
    }
}
