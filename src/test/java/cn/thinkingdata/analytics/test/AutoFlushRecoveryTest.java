package cn.thinkingdata.analytics.test;

import cn.thinkingdata.analytics.TDAnalytics;
import cn.thinkingdata.analytics.TDBatchConsumer;
import cn.thinkingdata.analytics.TDLoggerConsumer;
import cn.thinkingdata.analytics.exception.NeedRetryException;
import cn.thinkingdata.analytics.inter.ITDLogger;
import cn.thinkingdata.analytics.util.TDLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class AutoFlushRecoveryTest {
    private boolean previousLogging;
    private ITDLogger previousLogger;
    private final AtomicInteger failedDiagnostics = new AtomicInteger();

    @Before
    public void saveLoggingState() throws Exception {
        Field enabled = TDLogger.class.getDeclaredField("isPrintLog");
        enabled.setAccessible(true);
        previousLogging = enabled.getBoolean(null);
        Field logger = TDLogger.class.getDeclaredField("loggerInstance");
        logger.setAccessible(true);
        previousLogger = (ITDLogger) logger.get(null);
        TDAnalytics.enableLog(false);
    }

    @After
    public void restoreLoggingState() {
        TDAnalytics.setLogger(previousLogger);
        TDAnalytics.enableLog(previousLogging);
    }

    private void installFailingLogger() {
        TDAnalytics.setLogger(message -> {
            if (message.contains("Automatic flush failed:")) {
                failedDiagnostics.incrementAndGet();
                throw new IllegalStateException("diagnostic logger failed");
            }
        });
        TDAnalytics.enableLog(true);
    }

    @Test
    public void batchRetriesOnNextTickAfterNetworkFailure() throws Exception {
        verifyBatchRecovery(false);
    }

    @Test
    public void batchRetriesEvenWhenFailureLoggingThrows() throws Exception {
        verifyBatchRecovery(true);
    }

    private void verifyBatchRecovery(boolean brokenLogger) throws Exception {
        if (brokenLogger) installFailingLogger();
        RetryingBatch consumer = new RetryingBatch(batchConfig(true));
        try {
            consumer.add(Collections.<String, Object>singletonMap("event", "retained"));
            assertTrue("First send did not fail", consumer.failed.await(5, TimeUnit.SECONDS));
            assertTrue("Timer did not retry", consumer.sent.await(5, TimeUnit.SECONDS));
            assertEquals(2, consumer.attempts.get());
            assertEquals(1, consumer.sentSize);
            assertTrue(consumer.sentData.contains("retained"));
            if (brokenLogger) assertTrue(failedDiagnostics.get() > 0);
        } finally {
            consumer.close();
        }
    }

    @Test
    public void loggerRetriesOnNextTickAfterDirectoryRecovery() throws Exception {
        verifyLoggerRecovery(false);
    }

    @Test
    public void loggerRetriesEvenWhenFailureLoggingThrows() throws Exception {
        verifyLoggerRecovery(true);
    }

    private void verifyLoggerRecovery(boolean brokenLogger) throws Exception {
        if (brokenLogger) installFailingLogger();
        Path directory = Files.createTempDirectory("td-autoflush-recovery-");
        RecoveringLogger consumer = new RecoveringLogger(loggerConfig(directory, true));
        try {
            Files.delete(directory);
            consumer.add(Collections.<String, Object>singletonMap("event", "retained"));
            assertTrue("First flush did not fail", consumer.failed.await(5, TimeUnit.SECONDS));
            Files.createDirectory(directory);
            assertTrue("Timer did not retry", consumer.flushed.await(5, TimeUnit.SECONDS));
            int lines = 0;
            try (DirectoryStream<Path> files = Files.newDirectoryStream(directory)) {
                for (Path file : files) {
                    for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                        assertTrue(line.contains("retained"));
                        lines++;
                    }
                }
            }
            assertEquals(1, lines);
            if (brokenLogger) assertTrue(failedDiagnostics.get() > 0);
        } finally {
            Files.createDirectories(directory);
            consumer.close();
            deleteDirectory(directory);
        }
    }

    @Test
    public void explicitBatchFlushStillThrowsAndRetainsData() throws Exception {
        RetryingBatch consumer = new RetryingBatch(batchConfig(false));
        try {
            consumer.add(Collections.<String, Object>singletonMap("event", "retained"));
            try {
                consumer.flush();
                fail("Explicit flush must propagate failure");
            } catch (NeedRetryException expected) {
                // Existing caller-visible behavior is preserved.
            }
            consumer.flush();
            assertEquals(2, consumer.attempts.get());
            assertEquals(1, consumer.sentSize);
        } finally {
            consumer.close();
        }
    }

    @Test
    public void explicitLoggerFlushStillThrowsAndRetainsData() throws Exception {
        Path directory = Files.createTempDirectory("td-explicit-flush-");
        TDLoggerConsumer consumer = new TDLoggerConsumer(loggerConfig(directory, false));
        try {
            Files.delete(directory);
            consumer.add(Collections.<String, Object>singletonMap("event", "retained"));
            try {
                consumer.flush();
                fail("Explicit flush must propagate failure");
            } catch (RuntimeException expected) {
                // Existing caller-visible behavior is preserved.
            }
            Files.createDirectory(directory);
            consumer.flush();
            try (DirectoryStream<Path> files = Files.newDirectoryStream(directory)) {
                assertTrue(files.iterator().hasNext());
            }
        } finally {
            Files.createDirectories(directory);
            consumer.close();
            deleteDirectory(directory);
        }
    }

    @Test
    public void automaticFlushDoesNotRepeatSuccessfulWriteWhenLoggingThrows() throws Exception {
        AtomicInteger diagnostics = installFailingSuccessLogger();
        Path directory = Files.createTempDirectory("td-success-log-");
        CountDownLatch ticks = new CountDownLatch(2);
        TDLoggerConsumer consumer = new TDLoggerConsumer(loggerConfig(directory, true)) {
            @Override
            public void flush() {
                try {
                    super.flush();
                } finally {
                    ticks.countDown();
                }
            }
        };
        try {
            consumer.add(Collections.<String, Object>singletonMap("id", 1));
            assertTrue("Two automatic flushes did not run", ticks.await(5, TimeUnit.SECONDS));
            // Also exercise manual flush and repeated close after the timer ticks.
            consumer.flush();
            consumer.close();
            consumer.close();
            assertEquals(1, diagnostics.get());
            assertWrittenIds(directory, 1);
        } finally {
            TDAnalytics.enableLog(false);
            consumer.close();
            deleteDirectory(directory);
        }
    }

    @Test
    public void successfulExplicitWritesIgnoreLoggingFailure() throws Exception {
        AtomicInteger diagnostics = installFailingSuccessLogger();
        Path directory = Files.createTempDirectory("td-success-log-");
        TDLoggerConsumer.Config config = loggerConfig(directory, false);
        config.setBufferSize(20);
        TDLoggerConsumer consumer = new TDLoggerConsumer(config);
        try {
            consumer.add(Collections.<String, Object>singletonMap("id", 1));
            consumer.flush(); // Explicit flush must return normally after successful I/O.
            consumer.add(Collections.<String, Object>singletonMap("id", 2));
            consumer.add(Collections.<String, Object>singletonMap("id", 3));
            consumer.add(Collections.<String, Object>singletonMap("id", 4)); // Threshold flush.
            consumer.add(Collections.<String, Object>singletonMap("id", 5));
            consumer.close(); // Close flush must also succeed despite logging failures.
            consumer.close();
            assertEquals(3, diagnostics.get());
            assertWrittenIds(directory, 1, 2, 3, 4, 5);
        } finally {
            TDAnalytics.enableLog(false);
            consumer.close();
            deleteDirectory(directory);
        }
    }

    private static AtomicInteger installFailingSuccessLogger() {
        AtomicInteger diagnostics = new AtomicInteger();
        TDAnalytics.setLogger(message -> {
            if (message.contains("flush data:")) {
                diagnostics.incrementAndGet();
                throw new IllegalStateException("success logger failed");
            }
        });
        TDAnalytics.enableLog(true);
        return diagnostics;
    }

    private static void assertWrittenIds(Path directory, int... expected) throws Exception {
        java.util.List<Integer> actual = new java.util.ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(directory)) {
            for (Path file : files) {
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    actual.add(com.alibaba.fastjson2.JSON.parseObject(line).getIntValue("id"));
                }
            }
        }
        Collections.sort(actual);
        assertEquals(expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual.get(i).intValue());
        }
    }

    private static TDBatchConsumer.Config batchConfig(boolean autoFlush) {
        TDBatchConsumer.Config config = new TDBatchConsumer.Config();
        config.setAutoFlush(autoFlush);
        config.setInterval(1);
        config.setThrowException(true);
        return config;
    }

    private static TDLoggerConsumer.Config loggerConfig(Path directory, boolean autoFlush) {
        TDLoggerConsumer.Config config = new TDLoggerConsumer.Config(directory.toString());
        config.setAutoFlush(autoFlush);
        config.setInterval(1);
        config.setThrowException(true);
        return config;
    }

    private static class RetryingBatch extends TDBatchConsumer {
        final AtomicInteger attempts = new AtomicInteger();
        final CountDownLatch failed = new CountDownLatch(1);
        final CountDownLatch sent = new CountDownLatch(1);
        volatile String sentData;
        volatile int sentSize;

        RetryingBatch(Config config) throws Exception {
            super("http://unused.invalid", "test", config);
        }

        @Override
        public void httpSending(String data, int size) {
            if (attempts.incrementAndGet() == 1) {
                failed.countDown();
                throw new NeedRetryException("temporary network failure");
            }
            sentData = data;
            sentSize = size;
            sent.countDown();
        }
    }

    private static class RecoveringLogger extends TDLoggerConsumer {
        final CountDownLatch failed = new CountDownLatch(1);
        final CountDownLatch flushed = new CountDownLatch(1);

        RecoveringLogger(Config config) { super(config); }

        @Override
        public void flush() {
            try {
                super.flush();
                flushed.countDown();
            } catch (RuntimeException e) {
                failed.countDown();
                throw e;
            }
        }
    }

    private static void deleteDirectory(Path directory) throws Exception {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(directory)) {
            for (Path file : files) Files.delete(file);
        }
        Files.delete(directory);
    }
}
