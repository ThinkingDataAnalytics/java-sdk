package cn.thinkingdata.analytics;

import java.net.URISyntaxException;

/**
 * @deprecated please use TDDebugConsumer instead
 */
@Deprecated
public class DebugConsumer extends TDDebugConsumer {
    public DebugConsumer(String serverUrl, String appId) throws URISyntaxException {
        super(serverUrl, appId, true);
    }

    public DebugConsumer(String serverUrl, String appId, String deviceId) throws URISyntaxException {
        super(serverUrl, appId, true, deviceId);
    }

    public DebugConsumer(String serverUrl, String appId, boolean writeData) throws URISyntaxException {
        super(serverUrl, appId, writeData, null);
    }

    public DebugConsumer(String serverUrl, String appId, boolean writeData, String deviceId) throws URISyntaxException {
        super(serverUrl, appId, writeData, deviceId);
    }
}
