package io.github.mawngo.batch4j.impl;

import io.github.mawngo.batch4j.BatchProcessors;
import io.github.mawngo.batch4j.RunningProcessor;
import io.github.mawngo.batch4j.handlers.BatchMerger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class DisabledBatchingProcessorTest {
    @Test
    void shouldBatch() throws InterruptedException {
        final AtomicInteger sum = new AtomicInteger(0);
        final RunningProcessor<Integer> processor = BatchProcessors
                .newCallerRunBuilder((List<Integer> list) -> sum.updateAndGet(i -> i + list.size()))
                .disableIf(true)
                .build(BatchMerger.mergeToList())
                .run();
        for (int i = 0; i < 1_000_000; i++) {
            processor.put(i);
        }
        processor.close();
        Assertions.assertEquals(1_000_000, sum.get());
    }
}
