package cn.thinkingdata.analytics.test.functionTest;

import cn.thinkingdata.analytics.TDAnalytics;
import cn.thinkingdata.analytics.inter.ITDConsumer;
import org.junit.Test;
import java.util.*;
import static org.junit.Assert.*;

public class EventOwnershipTest {
    @Test
    public void retainedEventsKeepIndependentPropertiesAndPrecedence() throws Exception {
        final List<Map<String, Object>> events = new ArrayList<>();
        TDAnalytics sdk = new TDAnalytics(new ITDConsumer() {
            public void add(Map<String, Object> message) { events.add(message); }
            public void flush() {}
            public void close() {}
        });
        Map<String, Object> common = new HashMap<>();
        common.put("eventWins", "static");
        common.put("dynamicWins", "static");
        common.put("staticOnly", "static");
        common.put("nullWins", "static");
        common.put("#ip", "common-ip");
        sdk.setSuperProperties(common);
        Map<String, Object> dynamic = new HashMap<>();
        dynamic.put("eventWins", "dynamic");
        dynamic.put("dynamicWins", "dynamic");
        dynamic.put("nullWins", null);
        sdk.setDynamicSuperPropertiesTracker(() -> dynamic);
        Map<String, Object> input = new HashMap<>();
        input.put("eventWins", null);
        input.put("#ip", "event-ip");
        input.put("#time", null);
        Map<String, Object> original = new HashMap<>(input);
        sdk.track("account", "distinct", "first", Collections.unmodifiableMap(input));
        assertEquals(original, input);
        input.put("eventWins", "second");
        sdk.track("account", "distinct", "second", input);
        sdk.userSet("account", "distinct", null);
        Map<?, ?> first = (Map<?, ?>) events.get(0).get("properties");
        assertTrue(first.containsKey("eventWins"));
        assertNull(first.get("eventWins"));
        assertEquals("dynamic", first.get("dynamicWins"));
        assertEquals("static", first.get("staticOnly"));
        assertTrue(first.containsKey("nullWins"));
        assertNull(first.get("nullWins"));
        assertEquals("common-ip", first.get("#ip"));
        assertEquals("event-ip", events.get(0).get("#ip"));
        assertTrue(events.get(0).containsKey("#time"));
        assertNull(events.get(0).get("#time"));
        assertEquals("first", events.get(0).get("#event_name"));
        assertEquals("second", ((Map<?, ?>) events.get(1).get("properties")).get("eventWins"));
        assertTrue(((Map<?, ?>) events.get(2).get("properties")).isEmpty());
    }
}
