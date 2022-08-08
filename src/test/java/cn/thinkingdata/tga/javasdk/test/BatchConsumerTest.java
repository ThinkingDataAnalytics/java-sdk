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
                    new BatchConsumer("https://receiver-ta-demo.thinkingdata.cn", "cb1b413747ac4a2386c62a2575ac7746"));
        } catch (URISyntaxException e) {

        }
        ;
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
