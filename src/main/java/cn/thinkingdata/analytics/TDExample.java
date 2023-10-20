package cn.thinkingdata.analytics;

import cn.thinkingdata.analytics.inter.ITDConsumer;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

public class TDExample {
    static TDAnalytics tdAnalytics;

    public static ITDConsumer generateDebugConsumer() {
        ITDConsumer consumer = null;
        try {
            consumer = new TDDebugConsumer("url", "appId", "debug_device");
        } catch (Exception ignored){}
        return consumer;
    }

    public static ITDConsumer generateBatchConsumer() {
        ITDConsumer consumer = null;
        try {
            TDBatchConsumer.Config config = new TDBatchConsumer.Config();
            config.setBatchSize(20);
            config.setMaxCacheSize(10);
            consumer = new TDBatchConsumer("url", "appId", config);
        } catch (Exception ignored){}
        return consumer;
    }

    public static ITDConsumer generateLoggerConsumer() {
        TDLoggerConsumer.Config config = new TDLoggerConsumer.Config("./log");
        config.setFilenamePrefix("TD");
        return new TDLoggerConsumer(config);
    }

    public static void main(String[] args) {
        ITDConsumer consumer = null;

//        // Debug
//        consumer = generateDebugConsumer();

//        // Batch
//        consumer = generateBatchConsumer();
//
        // Log
        consumer = generateLoggerConsumer();


        if (consumer != null) {
            tdAnalytics = new TDAnalytics(consumer, false);
        }

        TDAnalytics.enableLog(true);

        String accountId = "accountId_123";
        String distinctId = "distinctId_aaa";

        try {
            tdAnalytics.track(accountId, distinctId, "login", null);
        } catch (Exception ignored) {}

        try {
            tdAnalytics.setSuperProperties(new HashMap<String, Object>(){
                {
                    put("super_key_1", "value_1");
                    put("super_key_2", "value_2");
                }
            });
        } catch (Exception ignored) {}

        try {
            tdAnalytics.trackFirst(accountId, distinctId, "event_2", "id_1", new HashMap<String, Object>(){
                {
                    put("name", "wang");
                    put("age", "18");
                }
            });
        } catch (Exception ignored) {}

        try {
            tdAnalytics.trackFirst(accountId, distinctId, "event_2", null, new HashMap<String, Object>(){
                {
                    put("name", "wang");
                    put("age", "18");
                    put("#first_check_id", "id_1");
                }
            });
        } catch (Exception e) {
            System.out.println("error: " + e);
        }

        try {
            tdAnalytics.trackUpdate(accountId, distinctId, "event_update", "eventId_1", new HashMap<String, Object>(){
                {
                    put("update_key_1", "old_value");
                    put("time", Calendar.getInstance().getTime());
                }
            });
        } catch (Exception ignored) {}

        try {
            tdAnalytics.trackUpdate(accountId, distinctId, "event_update", "eventId_1", new HashMap<String, Object>(){
                {
                    put("update_key_1", "new_value");
                    put("time", Calendar.getInstance().getTime());
                }
            });
        } catch (Exception ignored) {}

        try {
            tdAnalytics.trackOverwrite(accountId, distinctId, "event_overwrite", "eventId_2", new HashMap<String, Object>(){
                {
                    put("overwrite_key_1", "value_1");
                    put("time", Calendar.getInstance().getTime());
                }
            });
        } catch (Exception ignored) {}

        try {
            tdAnalytics.trackUpdate(accountId, distinctId, "event_overwrite", "eventId_2", new HashMap<String, Object>(){
                {
                    put("overwrite_key_2", "value_2");
                    put("time", Calendar.getInstance().getTime());
                }
            });
        } catch (Exception ignored) {}

        try {
            tdAnalytics.userSetOnce(accountId, distinctId, new HashMap<String, Object>(){
                {
                    put("name", "a");
                    put("userId", 1);
                    put("age", 18);
                }
            });
        } catch (Exception ignored) {}

        try {
            final ArrayList<String> fruits = new ArrayList<String>();
            fruits.add("apple");
            fruits.add("orange");
            tdAnalytics.userSet(accountId, distinctId, new HashMap<String, Object>(){
                {
                    put("name", "b");
                    put("fruits", fruits);
                }
            });
        } catch (Exception ignored) {}

        tdAnalytics.flush();

        // call when the service is turned off
        tdAnalytics.close();
    }
}
