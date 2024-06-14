package io.github.mawngo.batch4j.impl;

import io.github.mawngo.batch4j.BatchProcessors;
import io.github.mawngo.batch4j.ParallelProcessor;
import io.github.mawngo.batch4j.RunningProcessor;
import io.github.mawngo.batch4j.WaitingProcessor;
import io.github.mawngo.batch4j.handlers.BatchMerger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

class BatchingProcessorTest {
    @Test
    void shouldBatch() throws InterruptedException {
        final AtomicInteger sum = new AtomicInteger(0);
        final RunningProcessor<Integer> processor = BatchProcessors
                .newBuilder((List<Integer> list) -> sum.updateAndGet(i -> i + list.size()))
                .build(BatchMerger.mergeToList())
                .run();
        for (int i = 0; i < 1_000_000; i++) {
            processor.put(i);
        }
        processor.close();
        Assertions.assertEquals(1_000_000, sum.get());
    }

    @Test
    void shouldRestartable() throws InterruptedException {
        final AtomicInteger sum = new AtomicInteger(0);
        final WaitingProcessor<Integer, RunningProcessor<Integer>> base = BatchProcessors
                .newBuilder((List<Integer> list) -> sum.updateAndGet(i -> i + list.size()))
                .build(BatchMerger.mergeToList());
        RunningProcessor<Integer> processor = base.run();
        for (int i = 0; i < 1_000_000; i++) {
            processor.put(i);
        }
        processor.close();

        processor = base.run();
        for (int i = 0; i < 1_000_000; i++) {
            processor.put(i);
        }
        processor.close();
        Assertions.assertEquals(2_000_000, sum.get());
    }

    @Test
    void shouldBlockingBatch() throws InterruptedException {
        final AtomicInteger sum = new AtomicInteger(0);
        final RunningProcessor<Integer> processor = BatchProcessors
                .newBuilder((List<Integer> list) -> sum.updateAndGet(i -> i + list.size()))
                .maxItem(100)
                .build(BatchMerger.mergeToList())
                .run();

        for (int i = 0; i < 1_000_000; i++) {
            processor.put(i);
        }
        processor.close();
        Assertions.assertEquals(1_000_000, sum.get());
    }

    @Test
    void shouldBlockingBatchWithCollectionBased() throws InterruptedException {
        final AtomicInteger sum = new AtomicInteger(0);
        final RunningProcessor<Integer> processor = BatchProcessors
                .newBuilder((Set<Integer> list) -> sum.updateAndGet(i -> i + list.size()))
                .maxItem(100)
                .build(BatchMerger.mergeToSet(100))
                .run();

        for (int i = 0; i < 1_000_000; i++) {
            processor.put(i);
        }
        processor.close();
        Assertions.assertEquals(1_000_000, sum.get());
    }


    @Test
    void shouldFlush() throws InterruptedException {
        final AtomicInteger sum = new AtomicInteger(0);
        final RunningProcessor<Integer> processor = BatchProcessors
                .newBuilder((List<Integer> list) -> sum.updateAndGet(i -> i + list.size()))
                .blockWhileProcessing()
                .build(BatchMerger.mergeToList())
                .run();
        for (int i = 0; i < 1_000_000; i++) {
            processor.put(i);
        }
        processor.flush();
        Assertions.assertEquals(1_000_000, sum.get());
        processor.close();
    }

    @Test
    void shouldBlockingFlush() throws InterruptedException {
        final AtomicInteger sum = new AtomicInteger(0);
        final RunningProcessor<Integer> processor = BatchProcessors
                .newBuilder((List<Integer> list) -> sum.updateAndGet(i -> i + list.size()))
                .blockWhileProcessing()
                .maxItem(10)
                .build(BatchMerger.mergeToList())
                .run();

        for (int i = 0; i < 1_000_000; i++) {
            processor.put(i);
        }
        processor.flush();
        Assertions.assertEquals(1_000_000, sum.get());
        processor.close();
    }

    @Test
    void shouldBlockingBatchParallel() throws InterruptedException {
        final AtomicInteger sum = new AtomicInteger(0);
        final ParallelProcessor<Integer> processor = BatchProcessors
                .newBuilder((List<Integer> list) -> sum.updateAndGet(i -> i + list.size()))
                .maxItem(10)
                .blockWhileProcessing()
                .parallel("2")
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
                .parallel("2")
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
                .parallel("2")
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
