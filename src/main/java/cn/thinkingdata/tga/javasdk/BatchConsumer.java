package cn.thinkingdata.tga.javasdk;

import cn.thinkingdata.tga.javasdk.exception.IllegalDataException;
import cn.thinkingdata.tga.javasdk.exception.NeedRetryException;
import cn.thinkingdata.tga.javasdk.inter.Consumer;
import cn.thinkingdata.tga.javasdk.request.TABatchRequest;
import cn.thinkingdata.tga.javasdk.util.TACommonUtil;
import cn.thinkingdata.tga.javasdk.util.TALogger;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializeConfig;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static cn.thinkingdata.tga.javasdk.TAConstData.DEFAULT_DATE_FORMAT;
public class BatchConsumer implements Consumer {

    private final int batchSize;                         // flush event count each time
    private final int maxCacheSize;
    private final boolean isThrowException;
    private Timer autoFlushTimer;

    private final static int MAX_BATCH_SIZE = 1000;      // max limit of flush event count 
    private final Object messageLock = new Object();
    private final Object cacheLock = new Object();
    private List<Map<String, Object>> messageChannel;
    private final LinkedList<List<Map<String, Object>>> cacheBuffer = new LinkedList<>();
    private final TABatchRequest httpService;

    /**
     * init BatchConsumer with serverUrl and appId
     *
     * @param serverUrl serverUrl
     * @param appId     appId
     * @throws URISyntaxException exception
     */
    public BatchConsumer(String serverUrl, String appId) throws URISyntaxException {
        this(serverUrl, appId, 20, 0, false, 0, "gzip", 0, true);
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
        this(serverUrl, appId, 20, 0, false, 0, "gzip", 0, isThrowException);
    }

    /**
     * @param serverUrl serverUrl
     * @param appId     APP ID
     * @param config    BatchConsumer config
     * @throws URISyntaxException exception
     */
    public BatchConsumer(String serverUrl, String appId, Config config) throws URISyntaxException {
        this(serverUrl, appId, config.batchSize, config.timeout, config.autoFlush, config.interval, config.compress,
                config.maxCacheSize, config.isThrowException);
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
        this(serverUrl, appId, batchSize, timeout, autoFlush, interval, "gzip", 0, true);
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
    public BatchConsumer(String serverUrl, String appId, int batchSize, int timeout, boolean autoFlush, int interval,
                         String compress) throws URISyntaxException {
        this(serverUrl, appId, batchSize, timeout, autoFlush, interval, compress, 0, true);
    }

    /**
     * init BatchConsumer
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
    private BatchConsumer(String serverUrl, String appId, int batchSize, int timeout, boolean autoFlush, int interval,
                          String compress, int maxCacheSize, boolean isThrowException) throws URISyntaxException {
        TALogger.print("BatchConsumer Model,Server:"+serverUrl+" Appid:"+appId);
        this.messageChannel = new ArrayList<>();
        this.batchSize = batchSize < 0 ? 20 : Math.min(batchSize, MAX_BATCH_SIZE);
        this.maxCacheSize = maxCacheSize <= 0 ? 50 : maxCacheSize;
        this.isThrowException = isThrowException;
        URI uri = new URI(serverUrl);
        URI url = new URI(uri.getScheme(), uri.getAuthority(),
                "/sync_server", uri.getQuery(), uri.getFragment());
        this.httpService = new TABatchRequest(url, appId, timeout);
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
        private int batchSize = 20;
        private int interval = 3;
        private String compress = "gzip";
        private int timeout = 30000;
        private boolean autoFlush = false;
        private int maxCacheSize = 50;
        private boolean isThrowException = true;

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

        public void setTimeout(int timeout) {
            this.timeout = timeout;
        }

        public void setAutoFlush(boolean autoFlush) {
            this.autoFlush = autoFlush;
        }

        public void setMaxCacheSize(int maxCacheSize) {
            this.maxCacheSize = maxCacheSize;
        }

        public void setThrowException(boolean isThrowException) {
            this.isThrowException = isThrowException;
        }
    }

    @Override
    public void add(Map<String, Object> message) {
        String formatMsg = JSON.toJSONString(message, SerializeConfig.globalInstance, null, DEFAULT_DATE_FORMAT, TACommonUtil.fastJsonSerializerFeature());
        TALogger.print("collect data="+formatMsg);
        synchronized (messageLock) {
            messageChannel.add(message);
        }
        if (messageChannel.size() >= batchSize || cacheBuffer.size() > 0) {
            flushOnce();
        }
    }

    /**
     * upload all data in buffer immediately
     */
    @Override
    public void flush() {
        while (cacheBuffer.size() > 0 || messageChannel.size() > 0) {
            try {
                flushOnce();
            } catch (IllegalDataException ignore) {
            }
        }
    }



    public void flushOnce() {
        if (messageChannel.size() == 0 && cacheBuffer.size() == 0) {
            return;
        }

        synchronized (cacheLock) {
            synchronized (messageLock) {
                if (messageChannel.size() == 0 && cacheBuffer.size() == 0) {
                    return;
                }
                if (messageChannel.size() >= batchSize || cacheBuffer.size() == 0) {
                    cacheBuffer.add(messageChannel);
                    messageChannel = new ArrayList<>();
                }
            }

            List<Map<String, Object>> buffer = cacheBuffer.getFirst();

            try {
                String data = JSON.toJSONString(buffer, SerializeConfig.globalInstance, null, DEFAULT_DATE_FORMAT, TACommonUtil.fastJsonSerializerFeature());
                TALogger.print("flush data="+data);
                httpSending(data, buffer.size());
                cacheBuffer.removeFirst();
            } catch (NeedRetryException e) {
                TALogger.print(e.getLocalizedMessage());
                if (isThrowException) {
                    throw e;
                }
            } catch (IllegalDataException e) {
                TALogger.print(e.getLocalizedMessage());
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
