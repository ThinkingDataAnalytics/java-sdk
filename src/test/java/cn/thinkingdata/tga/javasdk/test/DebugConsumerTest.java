package cn.thinkingdata.tga.javasdk.test;

import cn.thinkingdata.tga.javasdk.DebugConsumer;
import cn.thinkingdata.tga.javasdk.LoggerConsumer;
import cn.thinkingdata.tga.javasdk.ThinkingDataAnalytics;
import cn.thinkingdata.tga.javasdk.exception.InvalidArgumentException;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;

public class DebugConsumerTest {
    ThinkingDataAnalytics taSDK;
    @Before
    public void  init()
    {
        try {
            ThinkingDataAnalytics.enableLog(true);
            taSDK = new ThinkingDataAnalytics(new DebugConsumer("serverUrl","appId","deviceId"));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void testDebugConsumer()
    {
        try {
            taSDK.track("Hale","","java_event",new HashMap<String,Object>(){{

            }});
            taSDK.flush();
        } catch (InvalidArgumentException e) {
            e.printStackTrace();
        }
    }
}
