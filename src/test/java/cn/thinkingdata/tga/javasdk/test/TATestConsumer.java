package cn.thinkingdata.tga.javasdk.test;

import cn.thinkingdata.tga.javasdk.inter.Consumer;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.Map;

/**
 * @author Sun Zeyu
 * @date 2021/6/9 10:36 上午
 */
public class TATestConsumer implements Consumer {

    private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private TaDataDo taData;

    @Override
    public void add(Map<String, Object> message) {
        JSONObject data = JSON.parseObject(JSON.toJSONStringWithDateFormat(message, DEFAULT_DATE_FORMAT));
        this.taData = new TaDataDo(data);
//        System.out.println(JSON.toJSONStringWithDateFormat(message, DEFAULT_DATE_FORMAT));
//        this.taData = JSONObject.parseObject(JSON.toJSONString(message), TaDataDo.class);
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    public TaDataDo getTaData() {
        return this.taData;
    }
}
