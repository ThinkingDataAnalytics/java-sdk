package cn.thinkingdata.analytics.test;

import cn.thinkingdata.analytics.TDLoggerConsumer;
import com.alibaba.fastjson2.JSON;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class LoggerConsumerConcurrencyTest {
    public static class BlockingValue {
        private final CountDownLatch entered;
        private final CountDownLatch release;

        BlockingValue(CountDownLatch entered, CountDownLatch release) {
            this.entered = entered;
            this.release = release;
        }

        public String getValue() throws InterruptedException {
            entered.countDown();
            if (!release.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("serialization timed out");
            }
            return "value";
        }
    }

    @Test
    public void blockedSerializationDoesNotBlockAddFlushOrClose() throws Exception {
        Path dir = Files.createTempDirectory("td-logger-concurrency-");
        TDLoggerConsumer consumer = new TDLoggerConsumer(dir.toString());
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            Future<?> slow = pool.submit(() -> consumer.add(Collections.singletonMap(
                    "slow", new BlockingValue(entered, release))));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            pool.submit(() -> {
                consumer.add(Collections.<String, Object>singletonMap("id", 1));
                consumer.flush();
                consumer.close();
            }).get(5, TimeUnit.SECONDS);
            release.countDown();
            slow.get(5, TimeUnit.SECONDS);
            consumer.add(Collections.<String, Object>singletonMap("id", 2));
            consumer.flush();
            List<String> lines = readLines(dir);
            assertEquals(1, lines.size());
            assertEquals(1, JSON.parseObject(lines.get(0)).getIntValue("id"));
        } finally {
            release.countDown();
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
            consumer.close();
            deleteFiles(dir);
        }
    }

    @Test
    public void concurrentAddsAndFlushesWriteEachEventOnce() throws Exception {
        Path dir = Files.createTempDirectory("td-logger-concurrency-");
        TDLoggerConsumer.Config config = new TDLoggerConsumer.Config(dir.toString());
        config.setBufferSize(128);
        config.setThrowException(true);
        TDLoggerConsumer consumer = new TDLoggerConsumer(config);
        ExecutorService pool = Executors.newFixedThreadPool(5);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<?>> tasks = new ArrayList<>();
            for (int t = 0; t < 4; t++) {
                final int base = t * 200;
                tasks.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < 200; i++) {
                        consumer.add(Collections.<String, Object>singletonMap("id", base + i));
                    }
                    return null;
                }));
            }
            tasks.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < 200; i++) consumer.flush();
                return null;
            }));
            start.countDown();
            for (Future<?> task : tasks) task.get(15, TimeUnit.SECONDS);
            consumer.close();
            List<String> lines = readLines(dir);
            assertEquals(800, lines.size());
            Set<Integer> ids = new HashSet<>();
            for (String line : lines) ids.add(JSON.parseObject(line).getIntValue("id"));
            assertEquals(800, ids.size());
            for (int i = 0; i < 800; i++) assertTrue(ids.contains(i));
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
            consumer.close();
            deleteFiles(dir);
        }
    }

    private static List<String> readLines(Path dir) throws Exception {
        List<String> lines = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir)) {
            for (Path file : files) lines.addAll(Files.readAllLines(file, StandardCharsets.UTF_8));
        }
        return lines;
    }

    private static void deleteFiles(Path dir) throws Exception {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir)) {
            for (Path file : files) Files.delete(file);
        }
        Files.delete(dir);
    }
}
