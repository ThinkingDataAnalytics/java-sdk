package cn.thinkingdata.tga.javasdk.test;

import cn.thinkingdata.tga.javasdk.LoggerConsumer;
import cn.thinkingdata.tga.javasdk.ThinkingDataAnalytics;
import cn.thinkingdata.tga.javasdk.exception.InvalidArgumentException;
import org.apache.commons.logging.Log;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class LoggerConsumerTest {
    ThinkingDataAnalytics taSDK;

    @Before
    public void init() {
        ThinkingDataAnalytics.enableLog(true);
        LoggerConsumer.Config config = new LoggerConsumer.Config("H:/log");
        config.setAutoFlush(true);
        taSDK = new ThinkingDataAnalytics(new LoggerConsumer(config), false, false);
    }

    @Test
    public void testLogConsumer() {
        try {
            Map<String, Object> properties = new HashMap<String, Object>();
            properties.put("name", "ta");
            taSDK.track("null", null, "java_event", properties);
            taSDK.flush();
        } catch (InvalidArgumentException e) {
            e.printStackTrace();
        }
    }
}
