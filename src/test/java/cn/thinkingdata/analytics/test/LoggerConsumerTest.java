package cn.thinkingdata.analytics.test;

import cn.thinkingdata.analytics.*;
import cn.thinkingdata.analytics.exception.InvalidArgumentException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class LoggerConsumerTest {
    TDAnalytics taSDK;

    @Before
    public void init() {
        TDAnalytics.enableLog(true);

        TDLoggerConsumer.Config config = new TDLoggerConsumer.Config("./log");
        config.setAutoFlush(true);
        config.setBufferSize(1024);

        taSDK = new TDAnalytics(new TDLoggerConsumer(config), false, false);
    }

    @Test
    public void testLogConsumer() {
        try {
            Map<String, Object> properties = new HashMap<String, Object>();
            properties.put("name", "ta");

            for (int i = 0; i < 20; i++) {
                taSDK.track("123", null, "java_event", properties);
            }
            taSDK.flush();
        } catch (InvalidArgumentException e) {
            e.printStackTrace();
        }
    }
}
