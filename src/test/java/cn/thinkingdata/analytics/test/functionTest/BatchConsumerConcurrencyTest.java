package cn.thinkingdata.analytics.test.functionTest;

import cn.thinkingdata.analytics.TDBatchConsumer;
import cn.thinkingdata.analytics.TDAnalytics;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchConsumerConcurrencyTest {

    @Test(timeout = 5000)
    public void concurrentAddsAreDeliveredWithoutLoss() throws Exception {
        TDAnalytics.enableLog(false);
        int threadCount = 8;
        int eventsPerThread = 100;
        CountingBatchConsumer consumer = new CountingBatchConsumer(1000);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);

        try {
            for (int thread = 0; thread < threadCount; thread++) {
                executor.submit(() -> {
                    ready.countDown();
                    await(start);
                    for (int i = 0; i < eventsPerThread; i++) {
                        consumer.add(event("concurrent_add"));
                    }
                });
            }

            Assert.assertTrue(ready.await(1, TimeUnit.SECONDS));
            start.countDown();
            executor.shutdown();
            Assert.assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));

            consumer.flush();
            Assert.assertEquals(threadCount * eventsPerThread, consumer.getDeliveredCount());
        } finally {
            executor.shutdownNow();
            consumer.close();
        }
    }

    @Test(timeout = 5000)
    public void closeRejectsEventsAfterClosingStartsAndWaitsForAcceptedSend() throws Exception {
        TDAnalytics.enableLog(false);
        BlockingBatchConsumer consumer = new BlockingBatchConsumer();
        Thread addThread = new Thread(() -> consumer.add(event("accepted_before_close")));
        addThread.start();
        Assert.assertTrue(consumer.awaitSendStarted());

        Thread closeThread = new Thread(consumer::close);
        closeThread.start();
        awaitLifecycleState(consumer, "CLOSING");

        consumer.add(event("rejected_during_close"));
        consumer.releaseSend();

        addThread.join(1000);
        closeThread.join(1000);
        Assert.assertFalse(addThread.isAlive());
        Assert.assertFalse(closeThread.isAlive());
        Assert.assertEquals(1, consumer.getDeliveredCount());
        Assert.assertEquals(1, consumer.getSendAttempts());
    }

    private static Map<String, Object> event(String name) {
        Map<String, Object> event = new HashMap<>();
        event.put("#type", "track");
        event.put("#event_name", name);
        event.put("properties", new HashMap<String, Object>());
        return event;
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void awaitLifecycleState(TDBatchConsumer consumer, String expected) throws Exception {
        Field lockField = TDBatchConsumer.class.getDeclaredField("stateLock");
        Field stateField = TDBatchConsumer.class.getDeclaredField("lifecycleState");
        lockField.setAccessible(true);
        stateField.setAccessible(true);
        Object stateLock = lockField.get(consumer);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (System.nanoTime() < deadline) {
            synchronized (stateLock) {
                if (expected.equals(stateField.get(consumer).toString())) {
                    return;
                }
            }
            Thread.yield();
        }
        Assert.fail("Lifecycle state did not become " + expected);
    }

    private static class CountingBatchConsumer extends TDBatchConsumer {
        private final AtomicInteger deliveredCount = new AtomicInteger();

        CountingBatchConsumer(int batchSize) throws URISyntaxException {
            super("http://127.0.0.1", "test-app-id", config(batchSize));
        }

        private static Config config(int batchSize) {
            Config config = new Config();
            config.setBatchSize(batchSize);
            return config;
        }

        @Override
        public void httpSending(String data, int dataSize) {
            deliveredCount.addAndGet(dataSize);
        }

        int getDeliveredCount() {
            return deliveredCount.get();
        }
    }

    private static class BlockingBatchConsumer extends CountingBatchConsumer {
        private final CountDownLatch sendStarted = new CountDownLatch(1);
        private final CountDownLatch allowSend = new CountDownLatch(1);
        private final AtomicInteger sendAttempts = new AtomicInteger();

        BlockingBatchConsumer() throws URISyntaxException {
            super(1);
        }

        @Override
        public void httpSending(String data, int dataSize) {
            sendAttempts.incrementAndGet();
            sendStarted.countDown();
            await(allowSend);
            super.httpSending(data, dataSize);
        }

        boolean awaitSendStarted() throws InterruptedException {
            return sendStarted.await(1, TimeUnit.SECONDS);
        }

        void releaseSend() {
            allowSend.countDown();
        }

        int getSendAttempts() {
            return sendAttempts.get();
        }
    }
}
