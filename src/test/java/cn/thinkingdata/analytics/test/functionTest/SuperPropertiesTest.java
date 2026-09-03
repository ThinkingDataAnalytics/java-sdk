package cn.thinkingdata.analytics.test.functionTest;

import cn.thinkingdata.analytics.TDAnalytics;
import cn.thinkingdata.analytics.inter.ITDConsumer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class SuperPropertiesTest {

    @Test
    public void returnedPropertiesDoNotExposeInternalState() {
        TDAnalytics analytics = new TDAnalytics(new NoOpConsumer());
        analytics.setSuperProperties(Collections.<String, Object>singletonMap("original", "value"));

        Map<String, Object> firstSnapshot = analytics.getSuperProperties();
        firstSnapshot.clear();
        firstSnapshot.put("external", "value");
        Map<String, Object> secondSnapshot = analytics.getSuperProperties();

        Assert.assertNotSame(firstSnapshot, secondSnapshot);
        Assert.assertEquals("value", secondSnapshot.get("original"));
        Assert.assertFalse(secondSnapshot.containsKey("external"));
    }

    @Test
    public void concurrentReadsAndWritesAreSafe() throws Exception {
        final TDAnalytics analytics = new TDAnalytics(new NoOpConsumer());
        final int threadCount = 4;
        final int iterations = 1000;
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int threadIndex = 0; threadIndex < threadCount; threadIndex++) {
            final int index = threadIndex;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        start.await();
                        for (int i = 0; i < iterations; i++) {
                            analytics.setSuperProperties(Collections.<String, Object>singletonMap(
                                    "key-" + index, i));
                            analytics.getSuperProperties().size();
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    }
                }
            });
        }

        start.countDown();
        executor.shutdown();
        Assert.assertTrue("Concurrent operations timed out", executor.awaitTermination(10, TimeUnit.SECONDS));
        Assert.assertNull(failure.get());
    }

    private static class NoOpConsumer implements ITDConsumer {
        @Override
        public void add(Map<String, Object> message) {
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}
