package cn.thinkingdata.tga.javasdk.util;

import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;


public class HttpRequestUtil {
    private static CloseableHttpClient httpClient = null;
    static {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setDefaultMaxPerRoute(80);
        cm.setMaxTotal(100);
        RequestConfig globalConfig = RequestConfig.custom()
                .setCookieSpec(CookieSpecs.IGNORE_COOKIES)
                .setConnectTimeout(600000)
                .setSocketTimeout(300000).build();
        httpClient = HttpClients.custom().setConnectionManager(cm).setDefaultRequestConfig(globalConfig).build();
    }

    public static CloseableHttpClient getHttpClient(){
        return httpClient;
    }

}
