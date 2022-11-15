package cn.thinkingdata.tga.javasdk;

import cn.thinkingdata.tga.javasdk.inter.Consumer;
import cn.thinkingdata.tga.javasdk.request.TADebugRequest;
import cn.thinkingdata.tga.javasdk.util.TACommonUtil;
import cn.thinkingdata.tga.javasdk.util.TALogger;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializeConfig;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static cn.thinkingdata.tga.javasdk.TAConstData.DEFAULT_DATE_FORMAT;

public class DebugConsumer implements Consumer {
     TADebugRequest httpService;
    /**
     * 创建指定接收端地址和 APP ID 的 DebugConsumer
     *
     * @param serverUrl 接收端地址
     * @param appId     APP ID
     * @throws URISyntaxException 上传地址异常
     */
    public DebugConsumer(String serverUrl, String appId) throws URISyntaxException {
        this(serverUrl, appId, true);
    }

    public DebugConsumer(String serverUrl, String appId, String deviceId) throws URISyntaxException {
        this(serverUrl, appId, true, deviceId);
    }

    public DebugConsumer(String serverUrl, String appId, boolean writeData) throws URISyntaxException {
        this(serverUrl, appId, writeData, null);
    }

    public DebugConsumer(String serverUrl, String appId, boolean writeData, String deviceId) throws URISyntaxException {
        TALogger.enableLog(true);
        TALogger.print("DebugConsumer Model,Server:"+serverUrl+"  Appid:"+appId);
        URI uri = new URI(serverUrl);
        URI restfulUri = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(),
                "/data_debug", uri.getQuery(), uri.getFragment());
        httpService = new TADebugRequest(restfulUri, appId, writeData, deviceId);
    }

    @Override
    public void add(Map<String, Object> message) {
        String data = JSON.toJSONString(message, SerializeConfig.globalInstance, null, DEFAULT_DATE_FORMAT, TACommonUtil.fastJsonSerializerFeature());
        TALogger.print("collect data="+data);
        try {
            httpService.send(data, 1);
        } catch (Exception e) {
            TALogger.print(e.getLocalizedMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {
        if (httpService != null) {
            httpService.close();
        }
    }
}