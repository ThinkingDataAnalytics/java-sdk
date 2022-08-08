package cn.thinkingdata.tga.javasdk.inter;

import java.util.Map;

/**
 * Created by quanjie on 2018/4/16.
 */
public interface Consumer {
    void add(Map<String, Object> message);

    void flush();

    void close();
}
