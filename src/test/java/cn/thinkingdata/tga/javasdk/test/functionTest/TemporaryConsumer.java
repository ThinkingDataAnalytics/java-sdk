package cn.thinkingdata.tga.javasdk.test.functionTest;

import cn.thinkingdata.tga.javasdk.inter.Consumer;
import cn.thinkingdata.tga.javasdk.util.TACommonUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;

import java.util.Map;

/**
 * @author Sun Zeyu
 * @date 2021/6/9 10:36
 */
public class TemporaryConsumer implements Consumer {

    private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private TemporaryEvent taData;

    @Override
    public void add(Map<String, Object> message) {
        String formatMsg = JSON.toJSONString(message, SerializeConfig.globalInstance, null, DEFAULT_DATE_FORMAT, TACommonUtil.fastJsonSerializerFeature());
        JSONObject data = JSON.parseObject(formatMsg);
        this.taData = new TemporaryEvent(data);
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    public TemporaryEvent getTaData() {
        return this.taData;
    }
}
