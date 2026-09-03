package cn.thinkingdata.analytics.test.functionTest;

import cn.thinkingdata.analytics.TDBatchConsumer;
import cn.thinkingdata.analytics.exception.NeedRetryException;
import org.junit.Assert;
import org.junit.Test;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class BatchConsumerFlushTest {

    @Test(timeout = 1000)
    public void flushReturnsAndRetainsBatchWhenNetworkIsUnavailable() throws Exception {
        FailingBatchConsumer consumer = new FailingBatchConsumer(false);
        consumer.add(event());

        consumer.flush();
        Assert.assertEquals(1, consumer.getSendAttempts());

        // The failed batch remains queued and can be retried by a later flush.
        consumer.flush();
        Assert.assertEquals(2, consumer.getSendAttempts());
    }

    @Test(expected = NeedRetryException.class)
    public void flushThrowsWhenThrowExceptionIsEnabled() throws Exception {
        FailingBatchConsumer consumer = new FailingBatchConsumer(true);
        consumer.add(event());
        consumer.flush();
    }

    private static Map<String, Object> event() {
        Map<String, Object> event = new HashMap<>();
        event.put("#type", "track");
        event.put("#event_name", "network_failure_test");
        event.put("properties", new HashMap<String, Object>());
        return event;
    }

    private static class FailingBatchConsumer extends TDBatchConsumer {
        private int sendAttempts;

        FailingBatchConsumer(boolean throwException) throws URISyntaxException {
            super("http://127.0.0.1", "test-app-id", config(throwException));
        }

        private static Config config(boolean throwException) {
            Config config = new Config();
            config.setThrowException(throwException);
            return config;
        }

        @Override
        public void httpSending(String data, int dataSize) {
            sendAttempts++;
            throw new NeedRetryException("network unavailable");
        }

        int getSendAttempts() {
            return sendAttempts;
        }
    }
}
