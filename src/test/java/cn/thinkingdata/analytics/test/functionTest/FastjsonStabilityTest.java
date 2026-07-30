package cn.thinkingdata.analytics.test.functionTest;

import cn.thinkingdata.analytics.util.TDCommonUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONValidator;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static cn.thinkingdata.analytics.TDConstData.DEFAULT_DATE_FORMAT;

/**
 * Stability checks for fastjson2 upgrade used by SDK serialize/parse paths.
 */
public class FastjsonStabilityTest {

    @Test
    public void serializeTrackPayload_roundTrip() {
        Map<String, Object> message = new LinkedHashMap<String, Object>();
        message.put("#account_id", "acc_001");
        message.put("#distinct_id", "dist_001");
        message.put("#event_name", "java_event");
        message.put("#type", "track");
        message.put("#time", new Date(0L));

        Map<String, Object> properties = new LinkedHashMap<String, Object>();
        properties.put("#lib", "tga_java_sdk");
        properties.put("name", "ta");
        properties.put("level", 10);
        properties.put("vip", true);
        properties.put("price", new BigDecimal("19.99"));
        properties.put("tags", Arrays.asList("a", "b", "c"));
        message.put("properties", properties);

        String json = JSON.toJSONString(message, DEFAULT_DATE_FORMAT, TDCommonUtil.fastJsonSerializerFeature());
        Assert.assertTrue(JSONValidator.from(json).validate());

        JSONObject parsed = JSON.parseObject(json);
        Assert.assertEquals("acc_001", parsed.getString("#account_id"));
        Assert.assertEquals("java_event", parsed.getString("#event_name"));
        Assert.assertTrue(parsed.getString("#time").matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}"));
        Assert.assertEquals("ta", parsed.getJSONObject("properties").getString("name"));
        Assert.assertEquals(Integer.valueOf(10), parsed.getJSONObject("properties").getInteger("level"));
        Assert.assertEquals(Boolean.TRUE, parsed.getJSONObject("properties").getBoolean("vip"));
    }

    @Test
    public void mapSortField_isStable() {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put("z", 1);
        props.put("a", 2);
        props.put("m", 3);

        String json1 = JSON.toJSONString(props, DEFAULT_DATE_FORMAT, TDCommonUtil.fastJsonSerializerFeature());
        String json2 = JSON.toJSONString(props, DEFAULT_DATE_FORMAT, TDCommonUtil.fastJsonSerializerFeature());
        Assert.assertEquals(json1, json2);
        Assert.assertTrue(json1.indexOf("\"a\"") < json1.indexOf("\"m\""));
        Assert.assertTrue(json1.indexOf("\"m\"") < json1.indexOf("\"z\""));
    }

    @Test
    public void parseServerResponse_shape() {
        String ok = "{\"code\":0,\"msg\":\"success\"}";
        String fail = "{\"code\":-1,\"msg\":\"invalid data\"}";

        JSONObject okJson = JSONObject.parseObject(ok);
        JSONObject failJson = JSONObject.parseObject(fail);
        Assert.assertEquals(0, okJson.getIntValue("code"));
        Assert.assertEquals(-1, failJson.getIntValue("code"));
        Assert.assertEquals("invalid data", failJson.getString("msg"));
    }

    @Test
    public void batchSerialize_repeatedlyStable() {
        List<Map<String, Object>> buffer = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 50; i++) {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("#account_id", "acc_" + i);
            item.put("#event_name", "evt");
            item.put("#type", "track");
            Map<String, Object> props = new LinkedHashMap<String, Object>();
            props.put("i", i);
            props.put("s", "v" + i);
            item.put("properties", props);
            buffer.add(item);
        }

        String first = null;
        for (int n = 0; n < 200; n++) {
            String json = JSON.toJSONString(buffer, DEFAULT_DATE_FORMAT, TDCommonUtil.fastJsonSerializerFeature());
            Assert.assertTrue(JSONValidator.from(json).validate());
            if (first == null) {
                first = json;
            } else {
                Assert.assertEquals(first, json);
            }
        }
    }

    @Test
    public void nestedObjectAndArray_roundTrip() {
        Map<String, Object> nested = new LinkedHashMap<String, Object>();
        nested.put("k1", "v1");
        nested.put("k2", Arrays.asList(1, 2, 3));

        Map<String, Object> props = new LinkedHashMap<String, Object>();
        props.put("obj", nested);
        props.put("arr", Arrays.asList(nested, "x"));

        String json = JSON.toJSONString(props, DEFAULT_DATE_FORMAT, TDCommonUtil.fastJsonSerializerFeature());
        JSONObject parsed = JSON.parseObject(json);
        Assert.assertEquals("v1", parsed.getJSONObject("obj").getString("k1"));
        Assert.assertEquals(3, parsed.getJSONObject("obj").getJSONArray("k2").size());
        Assert.assertEquals(2, parsed.getJSONArray("arr").size());
    }

    @Test
    public void stressSerialize_noException() {
        Map<String, Object> props = new HashMap<String, Object>();
        for (int i = 0; i < 100; i++) {
            props.put("k" + i, "v" + i);
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < 5000; i++) {
            String json = JSON.toJSONString(props, DEFAULT_DATE_FORMAT, TDCommonUtil.fastJsonSerializerFeature());
            Assert.assertNotNull(json);
            Assert.assertTrue(json.length() > 0);
        }
        long cost = System.currentTimeMillis() - start;
        System.out.println("[FastjsonStabilityTest] 5000 serialize cost(ms)=" + cost);
        Assert.assertTrue("serialize too slow: " + cost + "ms", cost < 10000);
    }
}
