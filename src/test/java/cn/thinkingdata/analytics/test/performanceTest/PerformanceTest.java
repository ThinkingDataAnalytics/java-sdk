package cn.thinkingdata.analytics.test.performanceTest;

import cn.thinkingdata.analytics.TDAnalytics;
import cn.thinkingdata.analytics.TDLoggerConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class PerformanceTest {
    @org.junit.Test
    public void trackDuration()
    {
        TDAnalytics ta = new TDAnalytics(new TDLoggerConsumer("./log"));
        Map<String,Object> map= new HashMap<>();
        for(int i = 0; i < 20; i++) {
            map.put("#dasdadsdddffddfasadffdffdffdfdffafd"+i,"#dasdadsdddffddfasadffdffdffdfdffafd"+i);
        }
        long time1 = System.currentTimeMillis();

        // add track ...
        try {
            for (int i = 0; i < 1; i++) {
                ta.track("a", "b", "eventName", map);
            }
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
        }

        long time2 = System.currentTimeMillis();
        System.out.println("time="+(time2-time1));
    }
}
