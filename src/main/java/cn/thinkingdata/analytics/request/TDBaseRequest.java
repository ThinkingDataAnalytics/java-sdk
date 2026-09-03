package cn.thinkingdata.analytics.request;

import com.alibaba.fastjson2.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import java.io.Closeable;
import java.net.URI;

import static cn.thinkingdata.analytics.TDConstData.LIB_VERSION;

abstract  public class TDBaseRequest implements Closeable {

    public URI getServerUri() {
        return serverUri;
    }
    private final URI serverUri;
    public String getAppId() {
        return appId;
    }
    private final String appId;
    public Integer getConnectTimeout() {
        return connectTimeout;
    }
    private Integer connectTimeout = null;
    public CloseableHttpClient getHttpClient() {
        return httpClient;
    }
    private CloseableHttpClient httpClient;
    public TDBaseRequest(URI server_uri, String appId, Integer timeout) {
        this(server_uri, appId);
        this.connectTimeout = timeout;
    }
    public TDBaseRequest(URI server_uri, String appId) {
        httpClient = TDHttpRequestClient.getHttpClient();
        this.serverUri = server_uri;
        this.appId = appId;
    }

    public synchronized void send(final String data, int dataSize)
    {
        HttpPost httpPost = new HttpPost(getServerUri());
        HttpEntity params = getHttpEntity(data);
        httpPost.setEntity(params);
        httpPost.addHeader("appid", getAppId());
        httpPost.addHeader("TA-Integration-Type", "Java");
        httpPost.addHeader("TA-Integration-Version", LIB_VERSION);
        httpPost.addHeader("TA-Integration-Count", String.valueOf(dataSize));

        if (this.getConnectTimeout() != null) {
            httpPost.setConfig(createRequestConfig());
        }
        sendRequest(httpPost);
    }

    private RequestConfig createRequestConfig() {
        int timeout = this.getConnectTimeout();
        int socketTimeout = timeout > Integer.MAX_VALUE - 5000 ? Integer.MAX_VALUE : timeout + 5000;
        return RequestConfig.custom()
                .setConnectTimeout(timeout)
                .setConnectionRequestTimeout(timeout)
                .setSocketTimeout(socketTimeout)
                .build();
    }
    abstract void sendRequest(HttpPost httpPost);
    abstract HttpEntity getHttpEntity(final String data);
    abstract void checkingRetCode(JSONObject resultJson);
    @Override
    public void close() {
        httpClient = null;
    }

}

