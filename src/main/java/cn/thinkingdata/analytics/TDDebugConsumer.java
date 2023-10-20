package cn.thinkingdata.analytics;

import cn.thinkingdata.analytics.inter.ITDConsumer;
import cn.thinkingdata.analytics.request.TDDebugRequest;
import cn.thinkingdata.analytics.util.TDCommonUtil;
import cn.thinkingdata.analytics.util.TDLogger;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.alibaba.fastjson.serializer.SerializeFilter;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static cn.thinkingdata.analytics.TDConstData.DEFAULT_DATE_FORMAT;

/**
 * Used to verify data before use SDK
 */
public class TDDebugConsumer implements ITDConsumer {
     TDDebugRequest httpService;

    /**
     * Construct DebugConsumer
     *
     * @param serverUrl receiver url
     * @param appId project ID in TE
     * @throws URISyntaxException exception
     */
    public TDDebugConsumer(String serverUrl, String appId) throws URISyntaxException {
        this(serverUrl, appId, true);
    }

    /**
     * Construct DebugConsumer
     *
     * @param serverUrl receiver url
     * @param appId project ID in TE
     * @param deviceId debug device ID
     * @throws URISyntaxException exception
     */
    public TDDebugConsumer(String serverUrl, String appId, String deviceId) throws URISyntaxException {
        this(serverUrl, appId, true, deviceId);
    }

    /**
     * Construct DebugConsumer
     *
     * @param serverUrl receiver url
     * @param appId project ID in TE
     * @param writeData Whether to write data to the TE database. default is false
     * @throws URISyntaxException exception
     */
    public TDDebugConsumer(String serverUrl, String appId, boolean writeData) throws URISyntaxException {
        this(serverUrl, appId, writeData, null);
    }

    /**
     * Construct DebugConsumer
     *
     * @param serverUrl receiver url
     * @param appId project ID in TE
     * @param writeData Whether to write data to the TE database. default is false
     * @param deviceId debug device ID
     * @throws URISyntaxException exception
     */
    public TDDebugConsumer(String serverUrl, String appId, boolean writeData, String deviceId) throws URISyntaxException {
        TDLogger.enableLog(true);
        TDLogger.println("DebugConsumer Model,Server:"+serverUrl+"  Appid:"+appId);
        URI uri = new URI(serverUrl);
        URI restfulUri = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(),
                "/data_debug", uri.getQuery(), uri.getFragment());
        httpService = new TDDebugRequest(restfulUri, appId, writeData, deviceId);
    }

    @Override
    public void add(Map<String, Object> message) {
        SerializeFilter[] filters = {};
        String data = JSON.toJSONString(message, SerializeConfig.globalInstance, filters, DEFAULT_DATE_FORMAT, TDCommonUtil.fastJsonSerializerFeature());
        TDLogger.println("collect data="+data);
        try {
            httpService.send(data, 1);
        } catch (Exception e) {
            TDLogger.println(e.getLocalizedMessage());
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