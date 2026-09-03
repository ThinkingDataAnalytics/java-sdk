package cn.thinkingdata.analytics.test;

import cn.thinkingdata.analytics.TDLoggerConsumer;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

public class LoggerConsumerExceptionTest {

    @Test
    public void fileCreationFailureIsIgnoredByDefault() throws Exception {
        TDLoggerConsumer consumer = consumerWithRemovedDirectory(false);

        consumer.add(Collections.<String, Object>singletonMap("key", "value"));
        consumer.flush();
    }

    @Test(expected = RuntimeException.class)
    public void fileCreationFailureIsThrownWhenEnabled() throws Exception {
        TDLoggerConsumer consumer = consumerWithRemovedDirectory(true);

        consumer.add(Collections.<String, Object>singletonMap("key", "value"));
        consumer.flush();
    }

    private static TDLoggerConsumer consumerWithRemovedDirectory(boolean isThrowException) throws Exception {
        Path directory = Files.createTempDirectory("td-logger-exception-test-");
        TDLoggerConsumer.Config config = new TDLoggerConsumer.Config(directory.toString());
        config.setThrowException(isThrowException);
        TDLoggerConsumer consumer = new TDLoggerConsumer(config);
        Files.delete(directory);
        return consumer;
    }
}
