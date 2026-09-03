package cn.thinkingdata.analytics.test.functionTest;

import cn.thinkingdata.analytics.TDAnalytics;
import cn.thinkingdata.analytics.exception.InvalidArgumentException;
import cn.thinkingdata.analytics.inter.ITDConsumer;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class NullPropertyTest {

    @Test
    public void eventPropertiesAllowNullValues() throws Exception {
        CapturingConsumer consumer = new CapturingConsumer();
        TDAnalytics analytics = new TDAnalytics(consumer);
        Map<String, Object> properties = new HashMap<>();
        properties.put("nullable_property", null);

        analytics.track("account-id", null, "null_property_event", properties);

        Map<String, Object> capturedProperties = capturedProperties(consumer);
        Assert.assertTrue(capturedProperties.containsKey("nullable_property"));
        Assert.assertNull(capturedProperties.get("nullable_property"));
    }

    @Test
    public void superPropertiesIgnoreNullKeysAndValues() throws Exception {
        CapturingConsumer consumer = new CapturingConsumer();
        TDAnalytics analytics = new TDAnalytics(consumer);
        Map<String, Object> superProperties = new HashMap<>();
        superProperties.put("valid_property", "valid-value");
        superProperties.put("nullable_property", null);
        superProperties.put(null, "null-key-value");

        analytics.setSuperProperties(superProperties);
        analytics.track("account-id", null, "super_property_event", null);

        Map<String, Object> capturedProperties = capturedProperties(consumer);
        Assert.assertEquals("valid-value", capturedProperties.get("valid_property"));
        Assert.assertFalse(capturedProperties.containsKey("nullable_property"));
        Assert.assertFalse(capturedProperties.containsKey(null));
    }

    @Test
    public void settingNullSuperPropertiesIsIgnored() throws Exception {
        CapturingConsumer consumer = new CapturingConsumer();
        TDAnalytics analytics = new TDAnalytics(consumer);

        analytics.setSuperProperties(null);
        analytics.track("account-id", null, "null_super_properties_event", null);

        Assert.assertNotNull(consumer.getMessage());
    }

    @Test
    public void trackFirstAllowsNullPropertiesWhenFirstCheckIdIsSet() throws Exception {
        CapturingConsumer consumer = new CapturingConsumer();
        TDAnalytics analytics = new TDAnalytics(consumer);

        analytics.trackFirst("account-id", null, "first_event", "first-check-id", null);

        Assert.assertEquals("first-check-id", consumer.getMessage().get("#first_check_id"));
    }

    @Test
    public void trackFirstDoesNotModifyOriginalProperties() throws Exception {
        CapturingConsumer consumer = new CapturingConsumer();
        TDAnalytics analytics = new TDAnalytics(consumer);
        Map<String, Object> properties = new HashMap<>();
        properties.put("original_property", "original-value");

        analytics.trackFirst("account-id", null, "first_event", "first-check-id", properties);

        Assert.assertFalse(properties.containsKey("#first_check_id"));
        Assert.assertEquals("first-check-id", consumer.getMessage().get("#first_check_id"));
    }

    @Test
    public void trackFirstRejectsMissingFirstCheckId() throws Exception {
        TDAnalytics analytics = new TDAnalytics(new CapturingConsumer());

        try {
            analytics.trackFirst("account-id", null, "first_event", null, null);
            Assert.fail("Expected InvalidArgumentException");
        } catch (InvalidArgumentException e) {
            Assert.assertEquals("#first_check_id key must set", e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> capturedProperties(CapturingConsumer consumer) {
        return (Map<String, Object>) consumer.getMessage().get("properties");
    }

    private static class CapturingConsumer implements ITDConsumer {
        private Map<String, Object> message;

        @Override
        public void add(Map<String, Object> message) {
            this.message = message;
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }

        Map<String, Object> getMessage() {
            return message;
        }
    }
}
