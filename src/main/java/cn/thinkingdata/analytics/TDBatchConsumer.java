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
    static final int DEFAULT_TIMEOUT = 30000;
    private final Object stateLock = new Object();
    private final Object sendLock = new Object();
    private List<Map<String, Object>> messageChannel;
    private final LinkedList<List<Map<String, Object>>> cacheBuffer = new LinkedList<List<Map<String, Object>>>();
    private final TDBatchRequest httpService;
    private LifecycleState lifecycleState = LifecycleState.OPEN;

    private enum LifecycleState {
        OPEN,
        CLOSING,
        CLOSED
    }

    /**
     * Construct BatchConsumer
     *
     * @param serverUrl serverUrl
     * @param appId     appId
     * @throws URISyntaxException exception
     */
    public TDBatchConsumer(String serverUrl, String appId) throws URISyntaxException {
        this(serverUrl, appId, 20, DEFAULT_TIMEOUT, false, 0, "gzip", 0, false);
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
        this(serverUrl, appId, 20, DEFAULT_TIMEOUT, false, 0, "gzip", 0, isThrowException);
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
        this(serverUrl, appId, batchSize, timeout, autoFlush, interval, "gzip", 0, false);
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
        this(serverUrl, appId, batchSize, timeout, autoFlush, interval, compress, 0, false);
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
        int effectiveTimeout = timeout <= 0 ? DEFAULT_TIMEOUT : timeout;
        this.httpService = new TDBatchRequest(url, appId, effectiveTimeout);
        this.httpService.setCompress(compress);
        if (autoFlush) {
            if (interval <= 0) {
                interval = 3;
            }
            autoFlushTimer = new Timer();
            autoFlushTimer.schedule(new TimerTask() {
                public void run() {
                    try {
                        flushOnce();
                    } catch (Exception e) {
                        // A background failure must not terminate the Timer thread.
                        // Explicit add/flush calls still follow isThrowException.
                        try {
                            TDLogger.println("Automatic flush failed: " + e);
                        } catch (Exception loggingException) {
                            // A custom logger must not prevent the next scheduled retry.
                        }
                    }
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
        protected int timeout = DEFAULT_TIMEOUT;
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
        protected boolean isThrowException = false;

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
            if ("none".equalsIgnoreCase(compress)) {
                this.compress = "none";
            } else if ("gzip".equalsIgnoreCase(compress)) {
                this.compress = "gzip";
            } else {
                TDLogger.println("Warning: compress is only supported for gzip");
                this.compress = "gzip";
            }
        }

        /**
         * Network timeout. Values less than or equal to zero use the default timeout.
         *
         * @param timeout timeout in milliseconds
         */
        public void setTimeout(int timeout) {
            this.timeout = timeout <= 0 ? DEFAULT_TIMEOUT : timeout;
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
        synchronized (stateLock) {
            if (lifecycleState != LifecycleState.OPEN) {
                TDLogger.println("SDK is already closed");
                return;
            }
        }
        String formatMsg = JSON.toJSONString(message, DEFAULT_DATE_FORMAT, TDCommonUtil.fastJsonSerializerFeature());
        TDLogger.println("collect data="+formatMsg);

        boolean shouldFlush;
        synchronized (stateLock) {
            if (lifecycleState != LifecycleState.OPEN) {
                TDLogger.println("SDK is already closed");
                return;
            }
            messageChannel.add(message);
            shouldFlush = messageChannel.size() >= batchSize || !cacheBuffer.isEmpty();
        }

        if (shouldFlush) {
            flushOnce();
        }
    }

    @Override
    public void flush() {
        synchronized (stateLock) {
            if (lifecycleState != LifecycleState.OPEN) {
                TDLogger.println("SDK is already closed");
                return;
            }
        }
        flushInternal(false);
    }

    private void flushInternal(boolean allowClosing) {
        while (true) {
            try {
                if (!flushOnceInternal(allowClosing)) {
                    return;
                }
            } catch (Exception e) {
                if (isThrowException) {
                    throw e;
                }
                TDLogger.println(e.getLocalizedMessage());
                return;
            }
        }
    }

    /**
     * Flush data once.
     */
    public void flushOnce() {
        flushOnceInternal(false);
    }

    /**
     * Flush data once.
     *
     * @return {@code true} if a batch was sent or discarded and the caller may
     * continue flushing; {@code false} if there is no data or the current batch
     * must be retried later
     */
    private boolean flushOnceInternal(boolean allowClosing) {
        synchronized (sendLock) {
            final List<Map<String, Object>> buffer;
            synchronized (stateLock) {
                if (lifecycleState == LifecycleState.CLOSED
                        || (!allowClosing && lifecycleState != LifecycleState.OPEN)) {
                    return false;
                }
                if (messageChannel.isEmpty() && cacheBuffer.isEmpty()) {
                    return false;
                }
                if (messageChannel.size() >= batchSize || cacheBuffer.isEmpty()) {
                    cacheBuffer.add(messageChannel);
                    messageChannel = new ArrayList<Map<String, Object>>();
                }
                buffer = cacheBuffer.getFirst();
            }

            try {
                String formatMsg = com.alibaba.fastjson2.JSON.toJSONString(buffer, DEFAULT_DATE_FORMAT, TDCommonUtil.fastJsonSerializerFeature());
                TDLogger.println("flush data="+formatMsg);
                httpSending(formatMsg, buffer.size());
                synchronized (stateLock) {
                    removeBufferLocked(buffer);
                }
                return true;
            } catch (NeedRetryException e) {
                TDLogger.println(e.getLocalizedMessage());
                if (isThrowException) {
                    throw e;
                }
                return false;
            } catch (IllegalDataException | NullPointerException e) {
                TDLogger.println(e.getLocalizedMessage());
                synchronized (stateLock) {
                    removeBufferLocked(buffer);
                }
                if (isThrowException) {
                    throw e;
                }
                return true;
            } finally {
                synchronized (stateLock) {
                    while (cacheBuffer.size() > maxCacheSize) {
                        cacheBuffer.removeFirst();
                    }
                }
            }
        }
    }

    private void removeBufferLocked(List<Map<String, Object>> buffer) {
        if (!cacheBuffer.isEmpty() && cacheBuffer.getFirst() == buffer) {
            cacheBuffer.removeFirst();
        } else {
            cacheBuffer.remove(buffer);
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
    public synchronized void close() {
        synchronized (stateLock) {
            if (lifecycleState == LifecycleState.CLOSED) {
                return;
            }
            lifecycleState = LifecycleState.CLOSING;
        }

        try {
            if (autoFlushTimer != null) {
                try {
                    autoFlushTimer.cancel();
                } catch (Exception e) {
                    if (isThrowException) {
                        throw e;
                    }
                    TDLogger.println(e.getLocalizedMessage());
                }
            }
            flushInternal(true);
        } finally {
            try {
                httpService.close();
            } finally {
                synchronized (stateLock) {
                    lifecycleState = LifecycleState.CLOSED;
                }
            }
        }
    }
}
