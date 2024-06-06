package cn.thinkingdata.analytics;

import cn.thinkingdata.analytics.exception.IllegalDataException;
import cn.thinkingdata.analytics.exception.NeedRetryException;
import cn.thinkingdata.analytics.inter.ITDConsumer;
import cn.thinkingdata.analytics.request.TDBatchRequest;
import cn.thinkingdata.analytics.util.TDCommonUtil;
import cn.thinkingdata.analytics.util.TDLogger;
import com.alibaba.fastjson2.JSON;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static cn.thinkingdata.analytics.TDConstData.DEFAULT_DATE_FORMAT;

/**
 * Data is reported using http
 */
public class TDBatchConsumer implements ITDConsumer {

    private final int batchSize;                         // flush event count each time
    private final int maxCacheSize;
    private final boolean isThrowException;
    private Timer autoFlushTimer;

    private final static int MAX_BATCH_SIZE = 1000;      // max limit of flush event count 
    private final Object messageLock = new Object();
    private final Object cacheLock = new Object();
    private List<Map<String, Object>> messageChannel;
    private final LinkedList<List<Map<String, Object>>> cacheBuffer = new LinkedList<List<Map<String, Object>>>();
    private final TDBatchRequest httpService;

    /**
     * Construct BatchConsumer
     *
     * @param serverUrl serverUrl
     * @param appId     appId
     * @throws URISyntaxException exception
     */
    public TDBatchConsumer(String serverUrl, String appId) throws URISyntaxException {
        this(serverUrl, appId, 20, 0, false, 0, "gzip", 0, true);
    }

    /**
     * Construct BatchConsumer
     *
     * @param serverUrl serverUrl
     * @param appId     APP ID
     * @param config    BatchConsumer config
     * @throws URISyntaxException exception
     */
    public TDBatchConsumer(String serverUrl, String appId, Config config) throws URISyntaxException {
        this(serverUrl, appId, config.batchSize, config.timeout, config.autoFlush, config.interval, config.compress, config.maxCacheSize, config.isThrowException);
    }

    /**
     * Construct BatchConsumer
     *
     * @param serverUrl        serverUrl
     * @param appId            APP ID
     * @param isThrowException throw exception or not
     * @throws URISyntaxException exception
     */
    @Deprecated
    protected TDBatchConsumer(String serverUrl, String appId, boolean isThrowException) throws URISyntaxException {
        this(serverUrl, appId, 20, 0, false, 0, "gzip", 0, isThrowException);
    }

    /**
     * Construct BatchConsumer
     *
     * @param serverUrl serverUrl
     * @param appId     APP ID
     * @param batchSize flush event count each time
     * @param timeout   http timeout (Unit: mill second)
     * @param autoFlush is auto flush or not
     * @param interval  auto flush spacing (Unit: second)
     * @throws URISyntaxException exception
     */
    @Deprecated
    protected TDBatchConsumer(String serverUrl, String appId, int batchSize, int timeout, boolean autoFlush, int interval) throws URISyntaxException {
        this(serverUrl, appId, batchSize, timeout, autoFlush, interval, "gzip", 0, true);
    }

    /**
     * Construct BatchConsumer
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
    @Deprecated
    protected TDBatchConsumer(String serverUrl, String appId, int batchSize, int timeout, boolean autoFlush, int interval, String compress) throws URISyntaxException {
        this(serverUrl, appId, batchSize, timeout, autoFlush, interval, compress, 0, true);
    }

    /**
     * Construct BatchConsumer
     *
     * @param serverUrl             serverUrl
     * @param appId                 APP ID
     * @param batchSize             flush event count each time
     * @param timeout               http timeout (Unit: mill second)
     * @param autoFlush             is auto flush or not
     * @param interval              auto flush spacing (Unit: second)
     * @param compress              compress type
     * @param maxCacheSize          max buffer count
     * @param isThrowException      is throw exception or not
     * @throws URISyntaxException   exception
     */
    protected TDBatchConsumer(String serverUrl, String appId, int batchSize, int timeout, boolean autoFlush, int interval,
                            String compress, int maxCacheSize, boolean isThrowException) throws URISyntaxException {
        TDLogger.println("BatchConsumer Model,Server:"+serverUrl+" Appid:"+appId);
        this.messageChannel = new ArrayList<Map<String, Object>>();
        this.batchSize = batchSize < 0 ? 20 : Math.min(batchSize, MAX_BATCH_SIZE);
        this.maxCacheSize = maxCacheSize <= 0 ? 50 : maxCacheSize;
        this.isThrowException = isThrowException;
        URI uri = new URI(serverUrl);
        URI url = new URI(uri.getScheme(), uri.getAuthority(),
                "/sync_server", uri.getQuery(), uri.getFragment());
        this.httpService = new TDBatchRequest(url, appId, timeout);
        this.httpService.setCompress(compress);
        if (autoFlush) {
            if (interval <= 0) {
                interval = 3;
            }
            autoFlushTimer = new Timer();
            autoFlushTimer.schedule(new TimerTask() {
                public void run() {
                    flushOnce();
                }
            }, 1000, interval * 1000L);
        }
    }

    /**
     * BatchConsumer config
     */
    public static class Config {
        /**
         * Batch size
         */
        protected int batchSize = 20;
        /**
         * Batch time interval
         */
        protected int interval = 3;
        /**
         * Http compress type. default is "gzip"
         */
        protected String compress = "gzip";
        /**
         * Http timeout
         */
        protected int timeout = 30000;
        /**
         * Enable auto flush
         */
        protected boolean autoFlush = false;
        /**
         * Max cache size
         */
        protected int maxCacheSize = 50;
        /**
         * Is throw exception
         */
        protected boolean isThrowException = true;

        /**
         * Construct batch consumer config
         */
        public Config() {
        }

        /**
         * @param batchSize flush event count each time
         */
        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        /**
         * @param interval auto flush spacing (Unit: second)
         */
        public void setInterval(int interval) {
            this.interval = interval;
        }

        /**
         * @param compress compress type
         */
        public void setCompress(String compress) {
            this.compress = compress;
        }

        /**
         * network timeout
         * @param timeout millSecond
         */
        public void setTimeout(int timeout) {
            this.timeout = timeout;
        }

        /**
         * Whether to allow automatic flushing
         * @param autoFlush enable or not
         */
        public void setAutoFlush(boolean autoFlush) {
            this.autoFlush = autoFlush;
        }

        /**
         * max cache buffer size
         * @param maxCacheSize number
         */
        public void setMaxCacheSize(int maxCacheSize) {
            this.maxCacheSize = maxCacheSize;
        }

        /**
         * Whether to throw exception
         * @param isThrowException enable or not
         */
        public void setThrowException(boolean isThrowException) {
            this.isThrowException = isThrowException;
        }
    }

    @Override
    public void add(Map<String, Object> message) {
        String formatMsg = JSON.toJSONString(message, DEFAULT_DATE_FORMAT, TDCommonUtil.fastJsonSerializerFeature());
        TDLogger.println("collect data="+formatMsg);
        synchronized (messageLock) {
            messageChannel.add(message);
        }
        if (messageChannel.size() >= batchSize || !cacheBuffer.isEmpty()) {
            flushOnce();
        }
    }

    @Override
    public void flush() {
        while (!cacheBuffer.isEmpty() || !messageChannel.isEmpty()) {
            try {
                flushOnce();
            } catch (IllegalDataException ignore) {
            }
        }
    }

    /**
     * Flush data once.
     */
    public void flushOnce() {
        if (messageChannel.isEmpty() && cacheBuffer.isEmpty()) {
            return;
        }

        synchronized (cacheLock) {
            synchronized (messageLock) {
                if (messageChannel.isEmpty() && cacheBuffer.isEmpty()) {
                    return;
                }
                if (messageChannel.size() >= batchSize || cacheBuffer.isEmpty()) {
                    cacheBuffer.add(messageChannel);
                    messageChannel = new ArrayList<Map<String, Object>>();
                }
            }

            List<Map<String, Object>> buffer = cacheBuffer.getFirst();

            try {
                String formatMsg = com.alibaba.fastjson2.JSON.toJSONString(buffer, DEFAULT_DATE_FORMAT, TDCommonUtil.fastJsonSerializerFeature());
//                SerializeFilter[] filters = {};
//                String data = JSON.toJSONString(buffer, SerializeConfig.globalInstance, filters, DEFAULT_DATE_FORMAT, TDCommonUtil.fastJsonSerializerFeature());
                TDLogger.println("flush data="+formatMsg);
                httpSending(formatMsg, buffer.size());
                cacheBuffer.removeFirst();
            } catch (NeedRetryException e) {
                TDLogger.println(e.getLocalizedMessage());
                if (isThrowException) {
                    throw e;
                }
            } catch (IllegalDataException e) {
                TDLogger.println(e.getLocalizedMessage());
                cacheBuffer.removeFirst();
                if (isThrowException) {
                    throw e;
                }
            } finally {
                if (cacheBuffer.size() > maxCacheSize) {
                    cacheBuffer.removeFirst();
                }
            }
        }
    }

    /**
     * Send data
     * @param data Event data
     * @param dataSize Data size
     */
    public void httpSending(final String data, final int dataSize) {
        httpService.send(data, dataSize);
    }

    @Override
    public void close() {
        if (autoFlushTimer != null) {
            try {
                autoFlushTimer.cancel();
            } catch (Exception ignore) {
            }
        }
        flush();
        if (httpService != null) {
            httpService.close();
        }
    }
}
