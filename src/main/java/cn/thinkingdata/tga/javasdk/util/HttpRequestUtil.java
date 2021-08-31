package cn.thinkingdata.tga.javasdk.util;

import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;


public class HttpRequestUtil {

    private static final PoolingHttpClientConnectionManager cm;
    private static final RequestConfig globalConfig;

    static {
        cm = new PoolingHttpClientConnectionManager();
        cm.setDefaultMaxPerRoute(80);
        cm.setMaxTotal(100);
        globalConfig = RequestConfig.custom()
                .setCookieSpec(CookieSpecs.IGNORE_COOKIES)
                .setConnectTimeout(30000)
                .setSocketTimeout(30000).build();
    }

    public static CloseableHttpClient getHttpClient() {
        return HttpClients.custom().setConnectionManager(cm).setDefaultRequestConfig(globalConfig).build();
    }

}
