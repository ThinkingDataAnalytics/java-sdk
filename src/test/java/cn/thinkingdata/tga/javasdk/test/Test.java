package cn.thinkingdata.tga.javasdk.test;

import cn.thinkingdata.tga.javasdk.LoggerConsumer;
import cn.thinkingdata.tga.javasdk.ThinkingDataAnalytics;
import cn.thinkingdata.tga.javasdk.exception.InvalidArgumentException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Test {
    @org.junit.Test
    public void test1()
    {
        ThinkingDataAnalytics ta = new ThinkingDataAnalytics(new LoggerConsumer("/Users/halewang/Desktop/log"));
        Map<String,Object> map= new HashMap<>();
        for(int i=0;i<20;i++)
        {
            map.put("#dasdadsdddffddfasadffdffdffdfdffafd"+i,"#dasdadsdddffddfasadffdffdffdfdffafd"+i);
        }
        long time1 = System.currentTimeMillis();
        Date date = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
//        try {
//            ta.track("hale","","eventName",map);
//        } catch (InvalidArgumentException e) {
//            throw new RuntimeException(e);
//        }
//        format.format(date);
        long time2 = System.currentTimeMillis();
        System.out.println("time="+(time2-time1));


    }
}
