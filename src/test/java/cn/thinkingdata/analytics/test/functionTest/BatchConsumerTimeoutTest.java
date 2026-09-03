package cn.thinkingdata.analytics.test.functionTest;

import cn.thinkingdata.analytics.BatchConsumer;
import cn.thinkingdata.analytics.TDBatchConsumer;
import cn.thinkingdata.analytics.request.TDBaseRequest;
import org.apache.http.client.config.RequestConfig;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class BatchConsumerTimeoutTest {

    @Test
    public void defaultConstructorUsesThirtySecondTimeout() throws Exception {
        TDBatchConsumer consumer = new TDBatchConsumer("http://127.0.0.1", "test-app-id");
        Assert.assertEquals(Integer.valueOf(30000), requestOf(consumer).getConnectTimeout());
    }

    @Test
    public void configDefaultsToSameTimeoutAsDefaultConstructor() throws Exception {
        TDBatchConsumer consumer = new TDBatchConsumer(
                "http://127.0.0.1",
                "test-app-id",
                new TDBatchConsumer.Config()
        );
        Assert.assertEquals(Integer.valueOf(30000), requestOf(consumer).getConnectTimeout());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void legacyDefaultConstructorUsesThirtySecondTimeout() throws Exception {
        BatchConsumer consumer = new BatchConsumer("http://127.0.0.1", "test-app-id");
        Assert.assertEquals(Integer.valueOf(30000), requestOf(consumer).getConnectTimeout());
    }

    @Test
    public void nonPositiveTimeoutUsesDefaultTimeout() throws Exception {
        Assert.assertEquals(Integer.valueOf(30000), requestWithTimeout(0).getConnectTimeout());
        Assert.assertEquals(Integer.valueOf(30000), requestWithTimeout(-1).getConnectTimeout());
    }

    @Test
    public void positiveTimeoutIsPreserved() throws Exception {
        Assert.assertEquals(Integer.valueOf(1234), requestWithTimeout(1234).getConnectTimeout());
    }

    @Test
    public void requestConfigSetsAllTimeouts() throws Exception {
        TDBaseRequest request = requestWithTimeout(1234);
        Method method = TDBaseRequest.class.getDeclaredMethod("createRequestConfig");
        method.setAccessible(true);

        RequestConfig config = (RequestConfig) method.invoke(request);

        Assert.assertEquals(1234, config.getConnectTimeout());
        Assert.assertEquals(1234, config.getConnectionRequestTimeout());
        Assert.assertEquals(6234, config.getSocketTimeout());
    }

    private static TDBaseRequest requestWithTimeout(int timeout) throws Exception {
        TDBatchConsumer.Config config = new TDBatchConsumer.Config();
        config.setTimeout(timeout);
        return requestOf(new TDBatchConsumer("http://127.0.0.1", "test-app-id", config));
    }

    private static TDBaseRequest requestOf(TDBatchConsumer consumer) throws Exception {
        Field field = TDBatchConsumer.class.getDeclaredField("httpService");
        field.setAccessible(true);
        return (TDBaseRequest) field.get(consumer);
    }
}
