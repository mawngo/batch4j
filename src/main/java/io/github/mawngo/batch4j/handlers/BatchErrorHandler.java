package io.github.mawngo.batch4j.handlers;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

@FunctionalInterface
public interface BatchErrorHandler<T> {
    void accept(T item, int count, Throwable e);

    static <T> BatchErrorHandler<T> from(BiConsumer<T, Throwable> consumer) {
        return (item, count, e) -> consumer.accept(item, e);
    }

    static <T> BatchErrorHandler<T> from(Consumer<T> consumer) {
        return (item, count, e) -> consumer.accept(item);
    }

    static <T> BatchErrorHandler<T> ignore() {
        return (item, count, e) -> {
            // Do nothing.
        };
    }

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

    static <T> BatchErrorHandler<T> logging() {
        return (item, count, e) -> {
            Logging.error("Error processing batch of {} items", count, e);
        };
    }
}
