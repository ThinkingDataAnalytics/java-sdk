package cn.thinkingdata.tga.javasdk.test;

import cn.thinkingdata.tga.javasdk.ThinkingDataAnalytics;
import org.apache.http.util.TextUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * @author Sun Zeyu
 * @date 2021/6/9 11:15 上午
 */

public class SdkUnitTest {
    private ThinkingDataAnalytics sdk;
    private TATestConsumer consumer;

    public void init() {
        consumer = new TATestConsumer();
        sdk = new ThinkingDataAnalytics(consumer,false,true);
    }
    @Test
    public void testAccountIdDistinctId() {
        init();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 23);
        properties.put("set", "female");


        //track
        Exception expect = null;
        try {
            sdk.track("", "", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNotNull(expect);
        Assert.assertEquals("accountId or distinctId must be provided.", expect.getMessage());

        expect = null;
        try {
            sdk.track("", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());
        Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getAccountId()));

        expect = null;
        try {
            sdk.track("accountId", "", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
        Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getDistinctId()));

        expect = null;
        try {
            sdk.track("accountId", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
        Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());

        //track_update
        expect = null;
        try {
            sdk.track_update("", "", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNotNull(expect);
        Assert.assertEquals("accountId or distinctId must be provided.", expect.getMessage());

        expect = null;
        try {
            sdk.track_update("", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());
        Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getAccountId()));


        expect = null;
        try {
            sdk.track_update("accountId", "", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
        Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getDistinctId()));

        expect = null;
        try {
            sdk.track_update("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
        Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());

        //track_overwrite
        expect = null;
        try {
            sdk.track_overwrite("", "", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNotNull(expect);
        Assert.assertEquals("accountId or distinctId must be provided.", expect.getMessage());

        expect = null;
        try {
            sdk.track_overwrite("", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());
        Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getAccountId()));

        expect = null;
        try {
            sdk.track_overwrite("accountId", "", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
        Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getDistinctId()));

        expect = null;
        try {
            sdk.track_overwrite("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
        Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());

        //user_set
        expect = null;
        try {
            sdk.user_set("", "", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNotNull(expect);
        Assert.assertEquals("accountId or distinctId must be provided.", expect.getMessage());

        expect = null;
        try {
            sdk.user_set("", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());
        Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getAccountId()));

        expect = null;
        try {
            sdk.user_set("accountId", "", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
        Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getDistinctId()));

        expect = null;
        try {
            sdk.user_set("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
        Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());

        //user_setOnce
        expect = null;
        try {
            sdk.user_setOnce("", "", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNotNull(expect);
        Assert.assertEquals("accountId or distinctId must be provided.", expect.getMessage());

        expect = null;
        try {
            sdk.user_setOnce("", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());
        Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getAccountId()));

        expect = null;
        try {
            sdk.user_setOnce("accountId", "", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
        Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getDistinctId()));

        expect = null;
        try {
            sdk.user_setOnce("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
        Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());

        //user_append
        expect = null;
        Map<String, Object> appendProperties = new HashMap<>();
        appendProperties.put("list", Arrays.asList("a", "b", "c"));
        try {
            sdk.user_append("", "", appendProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNotNull(expect);
        Assert.assertEquals("accountId or distinctId must be provided.", expect.getMessage());

        expect = null;
        try {
            sdk.user_append("", "distinctId", appendProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());
        Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getAccountId()));

        expect = null;
        try {
            sdk.user_append("accountId", "", appendProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
        Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getDistinctId()));


        expect = null;
        try {
            sdk.user_append("accountId", "distinctId", appendProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
        Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());

        //user_add
        expect = null;
        Map<String, Object> addProperties = new HashMap<>();
        appendProperties.put("age", 1);
        try {
            sdk.user_add("", "", addProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNotNull(expect);
        Assert.assertEquals("accountId or distinctId must be provided.", expect.getMessage());

        expect = null;
        try {
            sdk.user_add("", "distinctId", addProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());
        Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getAccountId()));


        expect = null;
        try {
            sdk.user_add("accountId", "", addProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
        Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getDistinctId()));

        expect = null;
        try {
            sdk.user_add("accountId", "distinctId", addProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
        Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());

        //user_unset
        expect = null;
        String[] ss = new String[]{"age", "sex"};
        try {
            sdk.user_unset("", "", ss);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNotNull(expect);
        Assert.assertEquals("accountId or distinctId must be provided.", expect.getMessage());

        expect = null;
        try {
            sdk.user_unset("", "distinctId", ss);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());
        Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getAccountId()));

        expect = null;
        try {
            sdk.user_unset("accountId", "", ss);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
        Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getDistinctId()));

        expect = null;
        try {
            sdk.user_unset("accountId", "distinctId", ss);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
        Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());

        //user_del
        expect = null;
        try {
            sdk.user_del("", "");
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNotNull(expect);
        Assert.assertEquals("accountId or distinctId must be provided.", expect.getMessage());

        expect = null;
        try {
            sdk.user_del("", "distinctId");
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());
        Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getAccountId()));

    }

    @Test
    public void testEventName() {
        init();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 23);
        properties.put("sex", "male");

        //track
        Exception expect = null;
        try {
            sdk.track("accountId", "", "", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNotNull(expect);
        Assert.assertEquals("The event name must be provided.", expect.getMessage());

        expect = null;
        try {
            sdk.track("accountId", "", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("eventName", consumer.getTaData().getEventName());

        //track_update
        expect = null;
        try {
            sdk.track_update("accountId", "", "", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNotNull(expect);
        Assert.assertEquals("The event name must be provided.", expect.getMessage());

        expect = null;
        try {
            sdk.track_update("accountId", "", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("eventName", consumer.getTaData().getEventName());

        //track_overwrite
        expect = null;
        try {
            sdk.track_overwrite("accountId", "", "", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNotNull(expect);
        Assert.assertEquals("The event name must be provided.", expect.getMessage());

        expect = null;
        try {
            sdk.track_overwrite("accountId", "", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("eventName", consumer.getTaData().getEventName());
    }

    @Test
    public void testEventId() {
        init();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 23);
        properties.put("sex", "male");

        //track_update
        Exception expect = null;
        try {
            sdk.track_update("accountId", "", "eventName", "", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNotNull(expect);
        Assert.assertEquals("The event id must be provided.", expect.getMessage());

        expect = null;
        try {
            sdk.track_update("accountId", "", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("eventId", consumer.getTaData().getEventId());

        //track_overwrite
        expect = null;
        try {
            sdk.track_overwrite("accountId", "", "eventName", "", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNotNull(expect);
        Assert.assertEquals("The event id must be provided.", expect.getMessage());

        expect = null;
        try {
            sdk.track_overwrite("accountId", "", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("eventId", consumer.getTaData().getEventId());
    }

    @Test
    public void testType() {
        init();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 23);
        properties.put("sex", "male");

        //track
        Exception expect = null;
        try {
            sdk.track("accountId", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("track", consumer.getTaData().getType());

        //track_update
        expect = null;
        try {
            sdk.track_update("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("track_update", consumer.getTaData().getType());

        //track_overwrite
        expect = null;
        try {
            sdk.track_overwrite("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("track_overwrite", consumer.getTaData().getType());

        //user_set
        expect = null;
        try {
            sdk.user_set("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("user_set", consumer.getTaData().getType());

        //user_setOnce
        expect = null;
        try {
            sdk.user_setOnce("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("user_setOnce", consumer.getTaData().getType());

        //user_append
        expect = null;
        Map<String, Object> appendProperties = new HashMap<>();
        appendProperties.put("list", Arrays.asList("a", "b", "c"));
        try {
            sdk.user_append("accountId", "distinctId", appendProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("user_append", consumer.getTaData().getType());

        //user_add
        expect = null;
        Map<String, Object> addProperties = new HashMap<>();
        appendProperties.put("age", 1);
        try {
            sdk.user_add("accountId", "distinctId", addProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("user_add", consumer.getTaData().getType());

        //user_unset
        expect = null;
        String[] ss = new String[]{"age", "sex"};
        try {
            sdk.user_unset("accountId", "distinctId", ss);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("user_unset", consumer.getTaData().getType());

        //user_del
        expect = null;
        try {
            sdk.user_del("accountId", "distinctId");
        } catch (Exception e) {
            expect = e;
            System.out.println(e.toString());
        }
        Assert.assertNull(expect);
        Assert.assertEquals("user_del", consumer.getTaData().getType());
    }

    @Test
    public void testTime() {
        init();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 23);
        properties.put("sex", "female");
        Date date = new Date();
        properties.put("#time", date);

        Exception expect = null;
        try {
            sdk.track("accountId", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertEquals(date, consumer.getTaData().getTime());

        expect = null;
        try {
            sdk.track_update("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertEquals(date, consumer.getTaData().getTime());

        expect = null;
        try {
            sdk.track_overwrite("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertEquals(date, consumer.getTaData().getTime());

        expect = null;
        try {
            sdk.user_set("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertEquals(date, consumer.getTaData().getTime());

        expect = null;
        try {
            sdk.user_setOnce("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertEquals(date, consumer.getTaData().getTime());

        properties.remove("#time");

        expect = null;
        try {
            sdk.track("accountId", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertNotNull(consumer.getTaData().getTime());

        expect = null;
        try {
            sdk.track_update("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertNotNull(consumer.getTaData().getTime());

        expect = null;
        try {
            sdk.track_overwrite("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertNotNull(consumer.getTaData().getTime());

        expect = null;
        try {
            sdk.user_set("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertNotNull(consumer.getTaData().getTime());

        expect = null;
        try {
            sdk.user_setOnce("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertNotNull(consumer.getTaData().getTime());

        expect = null;
        Map<String, Object> appendProperties = new HashMap<>();
        appendProperties.put("list", Arrays.asList("a", "b", "c"));
        appendProperties.put("#time", date);
        try {
            sdk.user_append("accountId", "distinctId", appendProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertEquals(date, consumer.getTaData().getTime());

        appendProperties.remove("#time");
        try {
            sdk.user_append("accountId", "distinctId", appendProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertNotNull(consumer.getTaData().getTime());

        expect = null;
        Map<String, Object> addProperties = new HashMap<>();
        addProperties.put("age", 1);
        addProperties.put("#time", date);
        try {
            sdk.user_add("accountId", "distinctId", addProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertEquals(date, consumer.getTaData().getTime());

        addProperties.remove("#time");
        try {
            sdk.user_add("accountId", "distinctId", addProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertNotNull(consumer.getTaData().getTime());
    }

    @Test
    public void testProperties() {
        init();
        //properties中传入#uuid、#ip、#time、#app_id，生成的数据中包含这些字段且都在properties外层，其他字段在properties内
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 23);
        properties.put("sex", "male");
        Date date = new Date();
        properties.put("#time", date);
        String uuid = UUID.randomUUID().toString();
        properties.put("#uuid", uuid);
        String ip = "192.168.10.10";
        properties.put("#ip", ip);
        String appId = "testAppid";
        properties.put("#app_id", appId);
        String firstCheckId = "firstCheckId";
        properties.put("#first_check_id", firstCheckId);
        String transactionProperty = "transactionProperty";
        properties.put("#transaction_property", transactionProperty);
        String importToolId = "importToolId";
        properties.put("#import_tool_id", importToolId);

        Exception expect = null;
        try {
            sdk.track("accountId", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(date, consumer.getTaData().getTime());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#uuid"));
        Assert.assertEquals(ip, consumer.getTaData().getIp());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#ip"));
        Assert.assertEquals(appId, consumer.getTaData().getAppId());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#app_id"));
        Assert.assertEquals(firstCheckId, consumer.getTaData().getFirstCheckId());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#firstCheckId"));
        Assert.assertEquals(transactionProperty, consumer.getTaData().getTransactionProperty());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#transaction_property"));
        Assert.assertEquals(importToolId, consumer.getTaData().getImportToolId());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#import_tool_id"));
        Assert.assertEquals(23, consumer.getTaData().getPropertyObj().get("age"));
        Assert.assertEquals("male", consumer.getTaData().getPropertyObj().get("sex"));

        expect = null;
        try {
            sdk.track_update("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(date, consumer.getTaData().getTime());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#uuid"));
        Assert.assertEquals(ip, consumer.getTaData().getIp());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#ip"));
        Assert.assertEquals(appId, consumer.getTaData().getAppId());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#app_id"));
        Assert.assertEquals(firstCheckId, consumer.getTaData().getFirstCheckId());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#firstCheckId"));
        Assert.assertEquals(transactionProperty, consumer.getTaData().getTransactionProperty());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#transaction_property"));
        Assert.assertEquals(importToolId, consumer.getTaData().getImportToolId());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#import_tool_id"));
        Assert.assertEquals(23, consumer.getTaData().getPropertyObj().get("age"));
        Assert.assertEquals("male", consumer.getTaData().getPropertyObj().get("sex"));

        expect = null;
        try {
            sdk.track_overwrite("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(date, consumer.getTaData().getTime());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#uuid"));
        Assert.assertEquals(ip, consumer.getTaData().getIp());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#ip"));
        Assert.assertEquals(appId, consumer.getTaData().getAppId());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#app_id"));
        Assert.assertEquals(firstCheckId, consumer.getTaData().getFirstCheckId());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#firstCheckId"));
        Assert.assertEquals(transactionProperty, consumer.getTaData().getTransactionProperty());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#transaction_property"));
        Assert.assertEquals(importToolId, consumer.getTaData().getImportToolId());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#import_tool_id"));
        Assert.assertEquals(23, consumer.getTaData().getPropertyObj().get("age"));
        Assert.assertEquals("male", consumer.getTaData().getPropertyObj().get("sex"));

        expect = null;
        try {
            sdk.user_set("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(date, consumer.getTaData().getTime());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#uuid"));
        Assert.assertEquals(ip, consumer.getTaData().getIp());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#ip"));
        Assert.assertEquals(appId, consumer.getTaData().getAppId());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#app_id"));
        Assert.assertEquals(23, consumer.getTaData().getPropertyObj().get("age"));
        Assert.assertEquals("male", consumer.getTaData().getPropertyObj().get("sex"));

        expect = null;
        try {
            sdk.user_setOnce("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(date, consumer.getTaData().getTime());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#uuid"));
        Assert.assertEquals(ip, consumer.getTaData().getIp());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#ip"));
        Assert.assertEquals(appId, consumer.getTaData().getAppId());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#app_id"));
        Assert.assertEquals(23, consumer.getTaData().getPropertyObj().get("age"));
        Assert.assertEquals("male", consumer.getTaData().getPropertyObj().get("sex"));

        expect = null;
        Map<String, Object> appendProperties = new HashMap<>();
        appendProperties.put("list", Arrays.asList("a", "b", "c"));
        appendProperties.put("#time", date);
        appendProperties.put("#uuid", uuid);
        appendProperties.put("#ip", ip);
        appendProperties.put("#app_id", appId);
        try {
            sdk.user_append("accountId", "distinctId", appendProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(date, consumer.getTaData().getTime());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#uuid"));
        Assert.assertEquals(ip, consumer.getTaData().getIp());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#ip"));
        Assert.assertEquals(appId, consumer.getTaData().getAppId());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#app_id"));

        expect = null;
        Map<String, Object> addProperties = new HashMap<>();
        addProperties.put("age", 1);
        addProperties.put("#time", date);
        addProperties.put("#uuid", uuid);
        addProperties.put("#ip", ip);
        addProperties.put("#app_id", appId);
        try {
            sdk.user_add("accountId", "distinctId", addProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(date, consumer.getTaData().getTime());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#uuid"));
        Assert.assertEquals(ip, consumer.getTaData().getIp());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#ip"));
        Assert.assertEquals(appId, consumer.getTaData().getAppId());
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("#app_id"));
    }

    @Test
    public void testUUID() {
        init();
        //未开启#uuid，并且未传入#uuid字段时，生成的数据中不包含#uuid
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 23);
        properties.put("sex", "male");
        Exception expect = null;
        try {
            sdk.track("accountId", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.track_update("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.track_overwrite("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.user_set("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.user_setOnce("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getUuid());

        expect = null;
        Map<String, Object> appendProperties = new HashMap<>();
        appendProperties.put("list", Arrays.asList("a", "b", "c"));
        try {
            sdk.user_append("accountId", "distinctId", appendProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getUuid());

        expect = null;
        Map<String, Object> addProperties = new HashMap<>();
        addProperties.put("age", 1);
        try {
            sdk.user_add("accountId", "distinctId", addProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getUuid());

        String uuid = UUID.randomUUID().toString();
        properties.put("#uuid", uuid);
        expect = null;
        try {
            sdk.track("accountId", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        System.out.println("uuid="+consumer.getTaData().getUuid());
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.track_update("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.track_overwrite("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.user_set("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.user_setOnce("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());

        expect = null;
        appendProperties.put("#uuid", uuid);
        try {
            sdk.user_append("accountId", "distinctId", appendProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());

        expect = null;
        addProperties.put("#uuid", uuid);
        try {
            sdk.user_add("accountId", "distinctId", addProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());

        properties.remove("#uuid");
        sdk = new ThinkingDataAnalytics(consumer, true);
        expect = null;
        try {
            sdk.track("accountId", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNotNull(consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.track_update("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNotNull(consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.track_overwrite("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNotNull(consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.user_set("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNotNull(consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.user_setOnce("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNotNull(consumer.getTaData().getUuid());

        expect = null;
        appendProperties.remove("#uuid");
        try {
            sdk.user_append("accountId", "distinctId", appendProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNotNull(consumer.getTaData().getUuid());

        expect = null;
        addProperties.remove("#uuid");
        try {
            sdk.user_add("accountId", "distinctId", addProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNotNull(consumer.getTaData().getUuid());
    }

    @Test
    public void testPublicProperties() {
        init();
        Map<String, Object> superProperties = new HashMap<>();
        superProperties.put("test", "public");
        sdk.setSuperProperties(superProperties);
        //properties中传入#uuid、#ip、#time、#app_id，生成的数据中包含这些字段且都在properties外层，其他字段在properties内
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 23);
        properties.put("sex", "male");

        Exception expect = null;
        try {
            sdk.track("accountId", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("public", consumer.getTaData().getPropertyObj().get("test"));

        expect = null;
        try {
            sdk.track_update("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("public", consumer.getTaData().getPropertyObj().get("test"));

        expect = null;
        try {
            sdk.track_overwrite("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("public", consumer.getTaData().getPropertyObj().get("test"));

        properties.put("test", "private");

        expect = null;
        try {
            sdk.track("accountId", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("private", consumer.getTaData().getPropertyObj().get("test"));

        expect = null;
        try {
            sdk.track_update("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("private", consumer.getTaData().getPropertyObj().get("test"));

        expect = null;
        try {
            sdk.track_overwrite("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("private", consumer.getTaData().getPropertyObj().get("test"));

        sdk.clearSuperProperties();
        properties.remove("test");

        expect = null;
        try {
            sdk.track("accountId", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("test"));

        expect = null;
        try {
            sdk.track_update("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("test"));

        expect = null;
        try {
            sdk.track_overwrite("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("test"));
    }
}
