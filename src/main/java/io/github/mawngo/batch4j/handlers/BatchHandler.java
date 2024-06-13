package io.github.mawngo.batch4j.handlers;

import java.util.function.Consumer;

@FunctionalInterface
public interface BatchHandler<T> {
    void accept(T item, int count);

    static <T> BatchHandler<T> from(Consumer<T> consumer) {
        return (item, count) -> consumer.accept(item);
    }
}
