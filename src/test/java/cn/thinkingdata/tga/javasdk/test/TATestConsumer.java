package cn.thinkingdata.tga.javasdk.test;

import cn.thinkingdata.tga.javasdk.inter.Consumer;
import cn.thinkingdata.tga.javasdk.util.TACommonUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;

import java.util.Map;

import static cn.thinkingdata.tga.javasdk.TAConstData.DEFAULT_DATE_FORMAT;

/**
 * @author Sun Zeyu
 * @date 2021/6/9 10:36 上午
 */
public class TATestConsumer implements Consumer {

    private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private TaDataDo taData;

    @Override
    public void add(Map<String, Object> message) {
        String formatMsg = JSON.toJSONString(message, SerializeConfig.globalInstance, null, DEFAULT_DATE_FORMAT, TACommonUtil.fastJsonSerializerFeature());
        JSONObject data = JSON.parseObject(formatMsg);
        this.taData = new TaDataDo(data);
//        System.out.println(formatMsg);
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
