package cn.thinkingdata.analytics;

import java.net.URISyntaxException;

/**
 * @deprecated please use TDBatchConsumer instead
 */
@Deprecated
public class BatchConsumer extends TDBatchConsumer {

    public BatchConsumer(String serverUrl, String appId) throws URISyntaxException {
        super(serverUrl, appId, 20, 0, false, 0, "gzip", 0, true);
    }

    /**
     * init BatchConsumer with serverUrl and appId
     *
     * @param serverUrl        serverUrl
     * @param appId            APP ID
     * @param isThrowException throw exception or not
     * @throws URISyntaxException exception
     */
    public BatchConsumer(String serverUrl, String appId, boolean isThrowException) throws URISyntaxException {
        super(serverUrl, appId, 20, 0, false, 0, "gzip", 0, isThrowException);
    }

    /**
     * @param serverUrl serverUrl
     * @param appId     APP ID
     * @param config    BatchConsumer config
     * @throws URISyntaxException exception
     */
    public BatchConsumer(String serverUrl, String appId, Config config) throws URISyntaxException {
        super(serverUrl, appId, config.batchSize, config.timeout, config.autoFlush, config.interval, config.compress, config.maxCacheSize, config.isThrowException);
    }

    /**
     * init BatchConsumer
     *
     * @param serverUrl serverUrl
     * @param appId     APP ID
     * @param batchSize flush event count each time
     * @param timeout   http timeout (Unit: mill second)
     * @param autoFlush is auto flush or not
     * @param interval  auto flush spacing (Unit: second)
     * @throws URISyntaxException exception
     */
    public BatchConsumer(String serverUrl, String appId, int batchSize, int timeout, boolean autoFlush, int interval) throws URISyntaxException {
        super(serverUrl, appId, batchSize, timeout, autoFlush, interval, "gzip", 0, true);
    }

    /**
     * init BatchConsumer
     *
     * @param serverUrl serverUrl
     * @param appId     APP ID
     * @param batchSize flush event count each time
     * @param timeout   http timeout (Unit: mill second)
     * @param autoFlush is auto flush or not
     * @param interval  auto flush spacing (Unit: second)
     * @param compress  compress type
     * @throws URISyntaxException exception
     */
    public BatchConsumer(String serverUrl, String appId, int batchSize, int timeout, boolean autoFlush, int interval, String compress) throws URISyntaxException {
        super(serverUrl, appId, batchSize, timeout, autoFlush, interval, compress, 0, true);
    }
}
