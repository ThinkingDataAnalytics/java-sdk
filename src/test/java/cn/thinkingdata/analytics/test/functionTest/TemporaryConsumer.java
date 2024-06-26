package cn.thinkingdata.analytics.test.functionTest;

import cn.thinkingdata.analytics.inter.ITDConsumer;
import cn.thinkingdata.analytics.util.TDCommonUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;

import java.util.Map;

/**
 * @author Sun Zeyu
 * @date 2021/6/9 10:36
 */
public class TemporaryConsumer implements ITDConsumer {

    private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private TemporaryEvent taData;

    @Override
    public void add(Map<String, Object> message) {
        String formatMsg = JSON.toJSONString(message, DEFAULT_DATE_FORMAT, TDCommonUtil.fastJsonSerializerFeature());
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
