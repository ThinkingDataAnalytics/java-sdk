package cn.thinkingdata.analytics.inter;

import java.util.Map;

public interface ITDConsumer {
    /**
     * track event
     * @param message event map
     */
    void add(Map<String, Object> message);

    /**
     * upload all data in buffer immediately
     */
    void flush();

    /**
     * close consumer
     */
    void close();
}
