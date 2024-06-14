package io.github.mawngo.batch4j.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

class BatchingIteratorTest {

    @Test
    void shouldBatch() {
        final int batchSize = 2;
        final long count = BatchingIterator.batchedStreamOf(Stream.of(1, 2, 3, 4, 5, 6), batchSize)
                .peek(list -> Assertions.assertEquals(list.size(), batchSize))
                .count();
        Assertions.assertEquals(count, 3);
    }
}
