package cn.thinkingdata.tga.javasdk.test;

import cn.thinkingdata.tga.javasdk.LoggerConsumer;
import cn.thinkingdata.tga.javasdk.ThinkingDataAnalytics;
import cn.thinkingdata.tga.javasdk.exception.InvalidArgumentException;
import org.apache.commons.logging.Log;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;

public class LoggerConsumerTest {
    ThinkingDataAnalytics taSDK;
    @Before
    public void  init()
    {
        ThinkingDataAnalytics.enableLog(true);
        LoggerConsumer.Config config= new LoggerConsumer.Config("/Users/halewang/Desktop/log");
        config.setAutoFlush(true);
        taSDK = new ThinkingDataAnalytics(new LoggerConsumer(config),false,false);
    }
    @Test
    public void testLogConsumer()
    {
        try {
            long time1 = System.currentTimeMillis();
            HashMap properties = new HashMap<String,Object>(){{
                for (int i=0;i<20;i++)
                {
                    put("#taqerreewrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrqere"+i,"taqerreewrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrqere"+i);
                }
            }};
            taSDK.track("null",null,"java_event",properties);

            long time2 =System.currentTimeMillis();
            System.out.println("duration="+(time2-time1));
        } catch (InvalidArgumentException e) {
            e.printStackTrace();
        }
    }
}
