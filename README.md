# Batch4j

Simple batch processing library for java .

## Installation

Not released yet.

Require java 8+.

Add library to gradle dependencies.

```groovy
dependencies {
    implementation 'io.github.mawngo:batch4j:0.1.0'
}
```

# Usage

Example usage:

```java

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Test
void example() throws InterruptedException {
    final AtomicInteger sum = new AtomicInteger(0);
    final RunningProcessor<Integer> processor = BatchProcessors
        .newBuilder((List<Integer> list) -> sum.updateAndGet(i -> i + list.size()))
        .build(BatchMerger.mergeToList())
        .maxWait(1000, TimeUnit.MILLISECONDS)       // maximum wait time before processing
        .maxItem(100)                               // maximum item in batch before processing
        .run();
    for (int i = 0; i < 1_000_000; i++) {
        processor.put(i);
    }
    processor.close();
    Assertions.assertEquals(1_000_000, sum.get());
}

@Test
void exampleParallel() throws InterruptedException {
    final AtomicInteger sum = new AtomicInteger(0);
    final ParallelProcessor<Integer> processor = BatchProcessors
        .newBuilder((List<Integer> list) -> sum.updateAndGet(i -> i + list.size()))
        .build(BatchMerger.mergeToList())
        .maxWait(1000, TimeUnit.MILLISECONDS)
        .maxItem(100)
        .parallelTo(Executors.newCachedThreadPool()) // specify executor for parallel processing
        //.parallel("2-10")                          // or specify parallel for auto creating one.
        .run();
    for (int i = 0; i < 1_000_000; i++) {
        processor.put(i);
    }
    processor.close();
    processor.closeExecutor(); // optional, closing backing executor.
    Assertions.assertEquals(1_000_000, sum.get());
}
```
