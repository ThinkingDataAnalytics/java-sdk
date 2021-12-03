package cn.thinkingdata.tga.javasdk;

import cn.thinkingdata.tga.javasdk.exception.IllegalDataException;
import cn.thinkingdata.tga.javasdk.exception.InvalidArgumentException;
import cn.thinkingdata.tga.javasdk.exception.NeedRetryException;
import cn.thinkingdata.tga.javasdk.util.HttpRequestUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.anarres.lzo.LzoAlgorithm;
import org.anarres.lzo.LzoCompressor;
import org.anarres.lzo.LzoLibrary;
import org.anarres.lzo.LzoOutputStream;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.http.util.TextUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

public class ThinkingDataAnalytics {

    private final Consumer consumer;
    private final Map<String, Object> superProperties;
    private final boolean enableUUID;

    private final static String LIB_VERSION = "1.8.1";
    private final static String LIB_NAME = "tga_java_sdk";

    private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private final static Pattern KEY_PATTERN = Pattern.compile("^(#[a-z][a-z0-9_]{0,49})|([a-z][a-z0-9_]{0,50})$", Pattern.CASE_INSENSITIVE);

    /**
     * 构造函数.
     *
     * @param consumer BatchConsumer, LoggerConsumer 等
     */
    public ThinkingDataAnalytics(final Consumer consumer) {
        this.consumer = consumer;
        this.enableUUID = false;
        this.superProperties = new ConcurrentHashMap<>();
    }

    public ThinkingDataAnalytics(final Consumer consumer, final boolean enableUUID) {
        this.consumer = consumer;
        this.enableUUID = enableUUID;
        this.superProperties = new ConcurrentHashMap<>();
    }

    private enum DataType {
        /**
         * 上报数据接口名
         */
        TRACK("track"),
        USER_SET("user_set"),
        USER_SET_ONCE("user_setOnce"),
        USER_ADD("user_add"),
        USER_DEL("user_del"),
        USER_UNSET("user_unset"),
        USER_APPEND("user_append"),
        TRACK_UPDATE("track_update"),
        TRACK_OVERWRITE("track_overwrite");

        private final String type;

        DataType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }

    /**
     * 删除用户，此操作不可逆
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @throws InvalidArgumentException 数据错误
     */
    public void user_del(String accountId, String distinctId)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.USER_DEL, null);
    }

    /**
     * 用户属性修改，只支持数字属性增加的接口
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_add(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.USER_ADD, properties);
    }

    /**
     * 设置用户属性. 如果该属性已经存在，该操作无效.
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_setOnce(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.USER_SET_ONCE, properties);
    }

    /**
     * 设置用户属性. 如果属性已经存在，则覆盖; 否则，新创建用户属性
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_set(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.USER_SET, properties);
    }

    /**
     * 删除用户指定的属性
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_unset(String accountId, String distinctId, String... properties)
            throws InvalidArgumentException {
        if (properties == null) {
            return;
        }
        Map<String, Object> prop = new HashMap<>();
        for (String s : properties) {
            prop.put(s, 0);
        }
        add(distinctId, accountId, DataType.USER_UNSET, prop);
    }

    /**
     * 用户的数组类型的属性追加
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_append(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.USER_APPEND, properties);
    }

    /**
     * 上报事件
     *
     * @param accountId  账号 ID
     * @param distinctId 访客ID
     * @param eventName  事件名称
     * @param properties 事件属性
     * @throws InvalidArgumentException 数据错误
     */
    public void track(String accountId, String distinctId, String eventName, Map<String, Object> properties)
            throws InvalidArgumentException {
        Map<String, Object> allProperties = new HashMap<>(superProperties);
        if (properties != null) {
            allProperties.putAll(properties);
        }
        add(distinctId, accountId, DataType.TRACK, eventName, null, allProperties);
    }

    /**
     * @param accountId  账号 ID
     * @param distinctId 访客ID
     * @param eventName  事件名称
     * @param eventId    事件ID
     * @param properties 事件属性
     * @throws InvalidArgumentException 数据错误
     */
    public void track_update(String accountId, String distinctId, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException {
        Map<String, Object> allProperties = new HashMap<>(superProperties);
        if (properties != null) {
            allProperties.putAll(properties);
        }
        add(distinctId, accountId, DataType.TRACK_UPDATE, eventName, eventId, allProperties);
    }

    /**
     * @param accountId  账号 ID
     * @param distinctId 访客ID
     * @param eventName  事件ID
     * @param eventId    事件ID
     * @param properties 事件属性
     * @throws InvalidArgumentException 数据错误
     */
    public void track_overwrite(String accountId, String distinctId, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException {
        Map<String, Object> allProperties = new HashMap<>(superProperties);
        if (properties != null) {
            allProperties.putAll(properties);
        }
        add(distinctId, accountId, DataType.TRACK_OVERWRITE, eventName, eventId, allProperties);
    }

    private void add(String distinctId, String accountId, DataType type, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, type, null, null, properties);
    }

    private void add(String distinctId, String accountId, DataType type, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException {
        if (TextUtils.isEmpty(accountId) && TextUtils.isEmpty(distinctId)) {
            throw new InvalidArgumentException("accountId or distinctId must be provided.");
        }

        Map<String, Object> finalProperties = (properties == null) ? new HashMap<String, Object>() : new HashMap<>(properties);
        Map<String, Object> event = new HashMap<>();

        //#uuid 只支持UUID标准格式xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        if (finalProperties.containsKey("#uuid")) {
            event.put("#uuid", finalProperties.get("#uuid"));
            finalProperties.remove("#uuid");
        } else if (enableUUID) {
            event.put("#uuid", UUID.randomUUID().toString());
        }
        assertProperties(type, finalProperties);
        if (!TextUtils.isEmpty(distinctId)) {
            event.put("#distinct_id", distinctId);
        }

        if (!TextUtils.isEmpty(accountId)) {
            event.put("#account_id", accountId);
        }

        if (finalProperties.containsKey("#app_id")) {
            event.put("#app_id", finalProperties.get("#app_id"));
            finalProperties.remove("#app_id");
        }

        if (finalProperties.containsKey("#time")) {
            event.put("#time", finalProperties.get("#time"));
            finalProperties.remove("#time");
        } else {
            event.put("#time", new Date());
        }

        //预置属性提取
        if (finalProperties.containsKey("#ip")) {
            event.put("#ip", finalProperties.get("#ip"));
            finalProperties.remove("#ip");
        }

        if (finalProperties.containsKey("#first_check_id")) {
            event.put("#first_check_id", finalProperties.get("#first_check_id"));
            finalProperties.remove("#first_check_id");
        }

        if (finalProperties.containsKey("#transaction_property")) {
            event.put("#transaction_property", finalProperties.get("#transaction_property"));
            finalProperties.remove("#transaction_property");
        }

        if (finalProperties.containsKey("#import_tool_id")) {
            event.put("#import_tool_id", finalProperties.get("#import_tool_id"));
            finalProperties.remove("#import_tool_id");
        }

        event.put("#type", type.getType());

        if (type == DataType.TRACK || type == DataType.TRACK_OVERWRITE || type == DataType.TRACK_UPDATE) {
            if (TextUtils.isEmpty(eventName)) {
                throw new InvalidArgumentException("The event name must be provided.");
            }
            if (type == DataType.TRACK_OVERWRITE || type == DataType.TRACK_UPDATE) {
                if (TextUtils.isEmpty(eventId)) {
                    throw new InvalidArgumentException("The event id must be provided.");
                }
            }
            if (!TextUtils.isEmpty(eventId)) {
                event.put("#event_id", eventId);
            }
            event.put("#event_name", eventName);
            finalProperties.put("#lib", LIB_NAME);
            finalProperties.put("#lib_version", LIB_VERSION);
        }

        event.put("properties", finalProperties);

        this.consumer.add(event);
    }

    private void assertProperties(DataType type, final Map<String, Object> properties) throws InvalidArgumentException {
        if (properties.size() == 0) {
            return;
        }

        for (Entry<String, Object> property : properties.entrySet()) {
            Object value = property.getValue();

            if (null == value) {
                continue;
            }

            if (KEY_PATTERN.matcher(property.getKey()).matches()) {
                if (DataType.USER_ADD == type) {
                    if (!(value instanceof Number) && !(property.getKey().startsWith("#"))) {
                        throw new InvalidArgumentException("Only Number is allowed for user_add. Invalid property: " + property.getKey());
                    }
                }
            } else {
                throw new InvalidArgumentException("Invalid key format: " + property.getKey());
            }
        }
    }

    /**
     * 清除公共事件属性
     */
    public void clearSuperProperties() {
        this.superProperties.clear();
    }

    /**
     * 设置公共事件属性. 公共事件属性会添加到每个事件的属性中上报
     *
     * @param properties 公共属性
     */
    public void setSuperProperties(Map<String, Object> properties) {
        this.superProperties.putAll(properties);
    }

    /**
     * 立即上报数据到接收端
     */
    public void flush() {
        this.consumer.flush();
    }

    /**
     * 关闭并退出 sdk 所有线程，停止前会清空所有本地数据
     */
    public void close() {
        this.consumer.close();
    }


    /**
     * LoggerConsumer 批量实时写本地文件，文件以天为分隔，需要搭配 LogBus 进行上传. 建议使用.
     */
    public static class LoggerConsumer implements Consumer {

        /**
         * 日志切分模式
         */
        public enum RotateMode {
            /**
             * 按日切分
             */
            DAILY,
            /**
             * 按小时切分
             */
            HOURLY
        }

        /**
         * LoggerConsumer 的配置信息
         */
        public static class Config {
            String logDirectory;
            RotateMode rotateMode = RotateMode.DAILY;
            String lockFileName;
            String fileNamePrefix;
            int interval = 0;
            int fileSize = 0;
            int bufferSize = 8192;
            boolean autoFlush = false;

            /**
             * 创建指定日志存放路径的 LoggerConsumer 配置
             *
             * @param logDirectory 日志存放路径
             */
            public Config(String logDirectory) {
                this(logDirectory, 0);
            }

            /**
             * 创建指定日志存放路径和日志大小的 LoggerConsumer 配置
             *
             * @param logDirectory 日志存放路径
             * @param fileSize     日志大小, 单位 MB, 默认为无限大
             */
            public Config(String logDirectory, int fileSize) {
                this.logDirectory = logDirectory;
                this.fileSize = fileSize;
            }

            /**
             * 设置日志切分模式
             *
             * @param rotateMode 日志切分模式
             */
            public void setRotateMode(RotateMode rotateMode) {
                this.rotateMode = rotateMode;
            }

            /**
             * 设置日志大小
             *
             * @param fileSize 日志大小，单位 MB
             */
            public void setFileSize(int fileSize) {
                this.fileSize = fileSize;
            }

            public void setLockFile(String lockFileName) {
                this.lockFileName = lockFileName;
            }

            /**
             * 设置缓冲区容量, 当超过该容量时会触发 flush 动作
             *
             * @param bufferSize 缓冲区大小，单位 byte.
             */
            public void setBufferSize(int bufferSize) {
                this.bufferSize = bufferSize;
            }

            /**
             * 设置用户名前缀
             *
             * @param fileNamePrefix 用户名前缀
             */
            public void setFilenamePrefix(String fileNamePrefix) {
                this.fileNamePrefix = fileNamePrefix;
            }

            /**
             * 设置自动保存
             *
             * @param autoFlush 是否自动保存
             */
            public void setAutoFlush(boolean autoFlush) {
                this.autoFlush = autoFlush;
            }

            /**
             * 自动保存间隔
             *
             * @param interval 自动保存任务间隔
             */
            public void setInterval(int interval) {
                this.interval = interval;
            }
        }

        private final String fileName;
        private final String lockFileName;
        private final int bufferSize;
        private final int fileSize;
        private Timer autoFlushTimer;

        private final StringBuffer messageBuffer = new StringBuffer();
        private final ThreadLocal<SimpleDateFormat> df;

        private LoggerFileWriter loggerWriter;

        /**
         * 创建指定配置信息的 LoggerConsumer
         *
         * @param config LoggerConsumer.Config instance.
         */
        public LoggerConsumer(final Config config) {
            if (config.logDirectory == null || config.logDirectory.length() == 0) {
                throw new RuntimeException("指定的目录路径不能为空！");
            }
            File dir = new File(config.logDirectory);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            if (!dir.isDirectory()) {
                throw new RuntimeException("指定的路径必须是个目录：" + config.logDirectory);
            }
            String fileNamePrefix = config.fileNamePrefix == null ? config.logDirectory + File.separator : config.logDirectory + File.separator + config.fileNamePrefix + ".";
            this.fileName = fileNamePrefix + "log.";
            this.fileSize = config.fileSize;
            this.lockFileName = config.lockFileName;
            this.bufferSize = config.bufferSize;

            final String dataFormat = config.rotateMode == RotateMode.HOURLY ? "yyyy-MM-dd-HH" : "yyyy-MM-dd";
            df = new ThreadLocal<SimpleDateFormat>() {
                @Override
                protected SimpleDateFormat initialValue() {
                    return new SimpleDateFormat(dataFormat);
                }
            };

            if (config.autoFlush) {
                if (config.interval <= 0) {
                    config.interval = 3;
                }
                autoFlushTimer = new Timer();
                autoFlushTimer.schedule(new TimerTask() {
                    public void run() {
                        flush();
                    }
                }, 1000, config.interval * 1000L);
            }
        }

        /**
         * 创建指定日志存放目录的 LoggerConsumer. 单个日志文件大小默认为 1G.
         *
         * @param logDirectory 日志存放目录
         */
        public LoggerConsumer(final String logDirectory) {
            this(new Config(logDirectory));
        }

        /**
         * 创建指定日志存放目录的 LoggerConsumer, 并指定单个日志文件大小.
         *
         * @param logDirectory 日志目录
         * @param fileSize     单个日志文件大小限制，单位 MB
         */
        public LoggerConsumer(final String logDirectory, int fileSize) {
            this(new Config(logDirectory, fileSize));
        }


        @Override
        public synchronized void add(Map<String, Object> message) {
            try {
                messageBuffer.append(JSON.toJSONStringWithDateFormat(message, DEFAULT_DATE_FORMAT));
                messageBuffer.append("\n");
            } catch (JSONException e) {
                throw new RuntimeException("Failed to add data", e);
            }

            if (messageBuffer.length() >= bufferSize) {
                this.flush();
            }
        }

        @Override
        public synchronized void flush() {
            if (messageBuffer.length() == 0) {
                return;
            }

            String fileName = getFileName();
            if (loggerWriter != null && !loggerWriter.getFileName().equals(fileName)) {
                LoggerFileWriter.removeInstance(loggerWriter);
                loggerWriter = null;
            }

            if (loggerWriter == null) {
                try {
                    loggerWriter = LoggerFileWriter.getInstance(fileName, lockFileName);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }

            if (loggerWriter.write(messageBuffer)) {
                messageBuffer.setLength(0);
            }
        }

        private String getFileName() {
            String resultPrefix = fileName + df.get().format(new Date()) + "_";
            int count = 0;
            String result = resultPrefix + count;
            if (fileSize > 0) {
                File target = new File(result);
                while (target.exists()) {
                    if ((target.length() / (1024 * 1024)) < fileSize) {
                        break;
                    }
                    result = resultPrefix + (++count);
                    target = new File(result);
                }
            }
            return result;
        }

        @Override
        public synchronized void close() {
            this.flush();
            if (loggerWriter != null) {
                LoggerFileWriter.removeInstance(loggerWriter);
                loggerWriter = null;
            }
            if (autoFlushTimer != null) {
                autoFlushTimer.cancel();
            }
        }

        private static class LoggerFileWriter {

            private final String fileName;
            private final FileOutputStream outputStream;
            private final FileOutputStream lockStream;
            private int refCount;

            private static final Map<String, LoggerFileWriter> instances = new HashMap<>();

            static LoggerFileWriter getInstance(final String fileName, final String lockFileName) throws FileNotFoundException {
                synchronized (instances) {
                    if (!instances.containsKey(fileName)) {
                        instances.put(fileName, new LoggerFileWriter(fileName, lockFileName));
                    }
                    LoggerFileWriter writer = instances.get(fileName);
                    writer.refCount++;
                    return writer;
                }
            }

            static void removeInstance(final LoggerFileWriter writer) {
                synchronized (instances) {
                    writer.refCount--;
                    if (writer.refCount == 0) {
                        writer.close();
                        instances.remove(writer.fileName);
                    }
                }
            }

            private LoggerFileWriter(final String fileName, final String lockFileName) throws FileNotFoundException {
                this.outputStream = new FileOutputStream(fileName, true);
                if (lockFileName != null) {
                    this.lockStream = new FileOutputStream(lockFileName, true);
                } else {
                    this.lockStream = this.outputStream;
                }

                this.fileName = fileName;
                this.refCount = 0;
            }

            private void close() {
                try {
                    outputStream.close();
                } catch (Exception e) {
                    throw new RuntimeException("fail to close tga outputStream.", e);
                }
            }

            String getFileName() {
                return this.fileName;
            }

            boolean write(final StringBuffer sb) {
                synchronized (this.lockStream) {
                    FileLock lock = null;
                    try {
                        final FileChannel channel = lockStream.getChannel();
                        lock = channel.lock(0, Long.MAX_VALUE, false);
                        outputStream.write(sb.toString().getBytes(StandardCharsets.UTF_8));
                    } catch (Exception e) {
                        throw new RuntimeException("failed to write tga file.", e);
                    } finally {
                        if (lock != null) {
                            try {
                                lock.release();
                            } catch (IOException e) {
                                throw new RuntimeException("failed to release tga file lock.", e);
                            }
                        }
                    }
                }
                return true;
            }
        }
    }

    public static class BatchConsumer implements Consumer {

        private final int batchSize;
        private final int maxCacheSize;
        private final boolean isThrowException;
        private Timer autoFlushTimer;

        private final static int MAX_BATCH_SIZE = 1000;
        private final Object messageLock = new Object();
        private final Object cacheLock = new Object();
        private List<Map<String, Object>> messageChannel;
        private final LinkedList<List<Map<String, Object>>> cacheBuffer = new LinkedList<>();
        private final HttpService httpService;


        /**
         * 创建指定接收端地址和 APP ID 的 BatchConsumer
         *
         * @param serverUrl 接收端地址
         * @param appId     APP ID
         * @throws URISyntaxException 上传地址异常
         */
        public BatchConsumer(String serverUrl, String appId) throws URISyntaxException {
            this(serverUrl, appId, 20, 0, false, 0, "gzip", 0, true);
        }

        /**
         * 创建指定接收端地址和 APP ID 的 BatchConsumer
         *
         * @param serverUrl        接收端地址
         * @param appId            APP ID
         * @param isThrowException 出错时是否抛出异常
         * @throws URISyntaxException 上传地址异常
         */
        public BatchConsumer(String serverUrl, String appId, boolean isThrowException) throws URISyntaxException {
            this(serverUrl, appId, 20, 0, false, 0, "gzip", 0, isThrowException);
        }

        /**
         * @param serverUrl 接收端地址
         * @param appId     APP ID
         * @param config    BatchConsumer配置类
         * @throws URISyntaxException 上传地址异常
         */
        public BatchConsumer(String serverUrl, String appId, Config config) throws URISyntaxException {
            this(serverUrl, appId, config.batchSize, config.timeout, config.autoFlush, config.interval, config.compress,
                    config.maxCacheSize, config.isThrowException);
        }


        /**
         * 创建指定接收端地址和 APP ID 的 BatchConsumer，并设定 batchSize, 网络请求 timeout, 发送频次
         *
         * @param serverUrl 接收端地址
         * @param appId     APP ID
         * @param batchSize 缓存数目上线
         * @param timeout   超时时长，单位 ms
         * @param autoFlush 自动上传开关
         * @param interval  自动上传间隔，单位秒
         * @throws URISyntaxException 上传地址异常
         */
        public BatchConsumer(String serverUrl, String appId, int batchSize, int timeout, boolean autoFlush, int interval) throws URISyntaxException {
            this(serverUrl, appId, batchSize, timeout, autoFlush, interval, "gzip", 0, true);
        }

        /**
         * 创建指定接收端地址和 APP ID 的 BatchConsumer，并设定 batchSize, 网络请求 timeout, 发送频次
         *
         * @param serverUrl 接收端地址
         * @param appId     APP ID
         * @param batchSize 缓存数目上线
         * @param timeout   超时时长，单位 ms
         * @param autoFlush 自动上传开关
         * @param interval  发送间隔，单位秒
         * @param compress  压缩方式
         * @throws URISyntaxException 上传地址异常
         */
        public BatchConsumer(String serverUrl, String appId, int batchSize, int timeout, boolean autoFlush, int interval,
                             String compress) throws URISyntaxException {
            this(serverUrl, appId, batchSize, timeout, autoFlush, interval, compress, 0, true);
        }

        private BatchConsumer(String serverUrl, String appId, int batchSize, int timeout, boolean autoFlush, int interval,
                              String compress, int maxCacheSize, boolean isThrowException) throws URISyntaxException {
            this.messageChannel = new ArrayList<>();
            this.batchSize = batchSize < 0 ? 20 : Math.min(batchSize, MAX_BATCH_SIZE);
            this.maxCacheSize = maxCacheSize <= 0 ? 50 : maxCacheSize;
            this.isThrowException = isThrowException;
            URI uri = new URI(serverUrl);
            URI url = new URI(uri.getScheme(), uri.getAuthority(),
                    "/sync_server", uri.getQuery(), uri.getFragment());
            this.httpService = new HttpService(url, appId, timeout);
            this.httpService.compress = compress;
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
         * BatchConsumer 的 配置类
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
             * @param batchSize BatchConsumer的flush条数，缓存数目
             */
            public void setBatchSize(int batchSize) {
                this.batchSize = batchSize;
            }

            /**
             * @param interval 自动发送间隔，单位秒
             */
            public void setInterval(int interval) {
                this.interval = interval;
            }

            /**
             * @param compress BatchConsumer的压缩方式
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
            synchronized (messageLock) {
                messageChannel.add(message);
            }
            if (messageChannel.size() >= batchSize || cacheBuffer.size() > 0) {
                flushOnce();
            }
        }

        /**
         * 将缓存的数据全部上传
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
                    String data = JSON.toJSONStringWithDateFormat(buffer, DEFAULT_DATE_FORMAT);
                    httpService.send(data, buffer.size());
                    cacheBuffer.removeFirst();
                } catch (NeedRetryException e) {
                    if (isThrowException) {
                        throw e;
                    }
                } catch (IllegalDataException e) {
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

    public static class DebugConsumer implements Consumer {
        HttpService httpService;

        /**
         * 创建指定接收端地址和 APP ID 的 DebugConsumer
         *
         * @param serverUrl 接收端地址
         * @param appId     APP ID
         * @throws URISyntaxException 上传地址异常
         */
        public DebugConsumer(String serverUrl, String appId) throws URISyntaxException {
            this(serverUrl, appId, true);
        }

        public DebugConsumer(String serverUrl, String appId, boolean writeData) throws URISyntaxException {
            URI uri = new URI(serverUrl);
            URI restfulUri = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(),
                    "/data_debug", uri.getQuery(), uri.getFragment());

            httpService = new HttpService(restfulUri, appId, writeData);
            httpService.setDebugMode();
        }

        @Override
        public void add(Map<String, Object> message) {
            String data = JSON.toJSONStringWithDateFormat(message, DEFAULT_DATE_FORMAT);

            try {
                httpService.send(data, 1);
            } catch (Exception e) {
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

    private static class HttpService implements Closeable {

        public enum ConsumeMode {
            BATCH, DEBUG
        }

        private ConsumeMode consumeMode = ConsumeMode.BATCH;

        private final URI serverUri;
        private final String appId;
        private Boolean writeData = true;
        private String compress = "gzip";
        private Integer connectTimeout = null;
        private static CloseableHttpClient httpClient;

        void setDebugMode() {
            this.consumeMode = ConsumeMode.DEBUG;
        }


        private HttpService(URI server_uri, String appId, Integer timeout) {
            this(server_uri, appId);
            this.connectTimeout = timeout;
        }

        private HttpService(URI server_uri, String appId, boolean writeData) {
            this(server_uri, appId);
            this.writeData = writeData;
        }

        private HttpService(URI server_uri, String appId) {
            if (httpClient == null) {
                httpClient = HttpRequestUtil.getHttpClient();
            }
            this.serverUri = server_uri;
            this.appId = appId;
        }

        private synchronized void send(final String data, int dataSize) {
            HttpPost httpPost = new HttpPost(serverUri);
            HttpEntity params = (consumeMode == ConsumeMode.BATCH) ? getBatchHttpEntity(data) : getDebugHttpEntity(data);
            httpPost.setEntity(params);
            httpPost.addHeader("appid", this.appId);
            httpPost.addHeader("TA-Integration-Type", "Java");
            httpPost.addHeader("TA-Integration-Version", LIB_VERSION);
            httpPost.addHeader("TA-Integration-Count", String.valueOf(dataSize));
            httpPost.addHeader("compress", compress);
            if (this.connectTimeout != null) {
                RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(connectTimeout + 5000).setConnectTimeout(connectTimeout).build();
                httpPost.setConfig(requestConfig);
            }
            for (int i = 0; ; ) {
                try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                    int statusCode = response.getStatusLine().getStatusCode();
                    if (statusCode != 200) {
                        throw new NeedRetryException("Cannot post message to " + this.serverUri + ", status code:" + statusCode);
                    }
                    String result = EntityUtils.toString(response.getEntity(), "UTF-8");
                    JSONObject resultJson = JSONObject.parseObject(result);
                    checkingRetCode(resultJson);
                    return;
                } catch (IOException | NeedRetryException e) {
                    if (i++ == 2) {
                        throw new NeedRetryException("Cannot post message to " + this.serverUri, e);
                    }
                } finally {
                    httpPost.releaseConnection();
                }
            }
        }

        UrlEncodedFormEntity getDebugHttpEntity(final String data) {
            List<NameValuePair> nameValuePairs = new ArrayList<>();
            nameValuePairs.add(new BasicNameValuePair("source", "server"));
            nameValuePairs.add(new BasicNameValuePair("appid", this.appId));
            nameValuePairs.add(new BasicNameValuePair("data", data));
            if (!this.writeData) {
                nameValuePairs.add(new BasicNameValuePair("dryRun", String.valueOf(1)));
            }
            return new UrlEncodedFormEntity(nameValuePairs, StandardCharsets.UTF_8);
        }

        HttpEntity getBatchHttpEntity(final String data) {
            try {
                byte[] dataCompressed;
                byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
                if ("lzo".equalsIgnoreCase(this.compress)) {
                    dataCompressed = lzoCompress(dataBytes);
                } else if ("lz4".equalsIgnoreCase(this.compress)) {
                    dataCompressed = lz4Compress(dataBytes);
                } else if ("none".equalsIgnoreCase(this.compress)) {
                    dataCompressed = dataBytes;
                } else {
                    dataCompressed = gzipCompress(dataBytes);
                }
                return new ByteArrayEntity(dataCompressed);
            } catch (IOException e) {
                throw new NeedRetryException("压缩数据失败！", e);
            }
        }

        private static byte[] lzoCompress(byte[] srcBytes) throws IOException {
            LzoCompressor compressor = LzoLibrary.getInstance().newCompressor(
                    LzoAlgorithm.LZO1X, null);

            ByteArrayOutputStream os = new ByteArrayOutputStream();
            LzoOutputStream cs = new LzoOutputStream(os, compressor);
            cs.write(srcBytes);
            cs.close();
            return os.toByteArray();
        }

        private static byte[] lz4Compress(byte[] srcBytes) throws IOException {
            LZ4Factory factory = LZ4Factory.fastestInstance();
            ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
            LZ4Compressor compressor = factory.fastCompressor();
            LZ4BlockOutputStream compressedOutput = new LZ4BlockOutputStream(
                    byteOutput, 2048, compressor);
            compressedOutput.write(srcBytes);
            compressedOutput.close();
            return byteOutput.toByteArray();
        }

        private static byte[] gzipCompress(byte[] srcBytes) throws IOException {
            GZIPOutputStream gzipOut = null;
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                gzipOut = new GZIPOutputStream(out);
                gzipOut.write(srcBytes);
                gzipOut.close();
                return out.toByteArray();
            } finally {
                if (gzipOut != null) {
                    gzipOut.close();
                }
            }

        }

        private void checkingRetCode(JSONObject resultJson) {
            if (this.consumeMode == ConsumeMode.DEBUG) {
                if (resultJson.getInteger("errorLevel") != 0) {
                    throw new IllegalDataException(resultJson.toJSONString());
                }
            } else if (this.consumeMode == ConsumeMode.BATCH) {
                int retCode = resultJson.getInteger("code");
                if (retCode != 0) {
                    if (retCode == -1) {
                        throw new IllegalDataException(resultJson.containsKey("msg") ? resultJson.getString("msg") : "invalid data format");
                    } else if (retCode == -2) {
                        throw new IllegalDataException(resultJson.containsKey("msg") ? resultJson.getString("msg") : "APP ID doesn't exist");
                    } else if (retCode == -3) {
                        throw new IllegalDataException(resultJson.containsKey("msg") ? resultJson.getString("msg") : "invalid ip transmission");
                    } else {
                        throw new IllegalDataException("Unexpected response return code: " + retCode);
                    }
                }
            }
        }

        @Override
        public void close() {
            if (httpClient != null) {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                httpClient = null;
            }
        }

    }
}
