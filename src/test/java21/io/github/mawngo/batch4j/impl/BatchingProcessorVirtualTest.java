package io.github.mawngo.batch4j.impl;

import io.github.mawngo.batch4j.BatchProcessors;
import io.github.mawngo.batch4j.ParallelProcessor;
import io.github.mawngo.batch4j.WaitingProcessor;
import io.github.mawngo.batch4j.handlers.BatchMerger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

class BatchingProcessorVirtualTest {
    @Test
    void shouldBlockingBatchParallel() throws InterruptedException {
        final AtomicInteger sum = new AtomicInteger(0);
        final ParallelProcessor<Integer> processor = BatchProcessors
                .newBuilder((List<Integer> list) -> sum.updateAndGet(i -> i + list.size()))
                .maxItem(10)
                .blockWhileProcessing()
                .parallelTo(Executors.newVirtualThreadPerTaskExecutor())
                .build(BatchMerger.mergeToList())
                .run();
        for (int i = 0; i < 1_000_000; i++) {
            processor.put(i);
        }
        processor.close();
        processor.closeExecutor();
        Assertions.assertEquals(1_000_000, sum.get());
    }

    @Test
    void shouldBatchParallel() throws InterruptedException {
        final AtomicInteger sum = new AtomicInteger(0);
        final ParallelProcessor<Integer> processor = BatchProcessors
                .newBuilder((List<Integer> list) -> sum.updateAndGet(i -> i + list.size()))
                .maxItem(100)
                .parallelTo(Executors.newVirtualThreadPerTaskExecutor())
                .build(BatchMerger.mergeToList())
                .run();
        for (int i = 0; i < 1_000_000; i++) {
            processor.put(i);
        }
        processor.close();
        processor.closeExecutor();
        Assertions.assertEquals(1_000_000, sum.get());
    }

    @Test
    void shouldBatchParallelRestartable() throws InterruptedException {
        final AtomicInteger sum = new AtomicInteger(0);
        final WaitingProcessor<Integer, ParallelProcessor<Integer>> base = BatchProcessors
                .newBuilder((List<Integer> list) -> sum.updateAndGet(i -> i + list.size()))
                .parallelTo(Executors.newVirtualThreadPerTaskExecutor())
                .build(BatchMerger.mergeToList());
        ParallelProcessor<Integer> processor = base.run();

        for (int i = 0; i < 1_000_000; i++) {
            processor.put(i);
        }
        processor.close();
        processor = base.run();
        for (int i = 0; i < 1_000_000; i++) {
            processor.put(i);
        }
        processor.close();
        processor.closeExecutor();
        Assertions.assertEquals(2_000_000, sum.get());
    }
}
