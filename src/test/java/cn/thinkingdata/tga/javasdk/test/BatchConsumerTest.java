package cn.thinkingdata.tga.javasdk.test;

import cn.thinkingdata.tga.javasdk.BatchConsumer;
import cn.thinkingdata.tga.javasdk.LoggerConsumer;
import cn.thinkingdata.tga.javasdk.ThinkingDataAnalytics;
import cn.thinkingdata.tga.javasdk.exception.InvalidArgumentException;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;

public class BatchConsumerTest {
    ThinkingDataAnalytics taSDK;

    @Before
    public void init() {
        try {
            ThinkingDataAnalytics.enableLog(true);
            taSDK = new ThinkingDataAnalytics(
                    new BatchConsumer("your server url", "your app id"));
        } catch (URISyntaxException e) {

        }
    }

    @Test
    public void testBatchConsumer() {
        try {
            taSDK.track("Hale", "", "java_event", new HashMap<String, Object>() {
                {
                    put("a", "test");
                    put("b", new Date());
                }
            });
            taSDK.flush();
        } catch (InvalidArgumentException e) {
            e.printStackTrace();
        }
    }
}
