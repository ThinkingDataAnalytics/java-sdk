package cn.thinkingdata.tga.javasdk;

import cn.thinkingdata.tga.javasdk.exception.InvalidArgumentException;
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
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

public class ThinkingDataAnalytics {

    private final Consumer consumer;
    private final Map<String, Object> superProperties;

    private final static String LIB_VERSION = "1.5.0";
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
        this.superProperties = new ConcurrentHashMap<String, Object>();
    }

    private enum DataType {
        TRACK("track"),
        USER_SET("user_set"),
        USER_SET_ONCE("user_setOnce"),
        USER_ADD("user_add"),
        USER_DEL("user_del"),
        USER_UNSET("user_unset"),
        USER_APPEND("user_append");

        private String type;

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
     * @param account_id  账号 ID
     * @param distinct_id 访客 ID
     * @throws InvalidArgumentException 数据错误
     */
    public void user_del(String account_id, String distinct_id)
            throws InvalidArgumentException {
        __add(distinct_id, account_id, DataType.USER_DEL, null);
    }

    /**
     * 用户属性修改，只支持数字属性增加的接口
     *
     * @param account_id  账号 ID
     * @param distinct_id 访客 ID
     * @param properties  用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_add(String account_id, String distinct_id, Map<String, Object> properties)
            throws InvalidArgumentException {
        __add(distinct_id, account_id, DataType.USER_ADD, properties);
    }

    /**
     * 设置用户属性. 如果该属性已经存在，该操作无效.
     *
     * @param account_id  账号 ID
     * @param distinct_id 访客 ID
     * @param properties  用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_setOnce(String account_id, String distinct_id, Map<String, Object> properties)
            throws InvalidArgumentException {
        __add(distinct_id, account_id, DataType.USER_SET_ONCE, properties);
    }

    /**
     * 设置用户属性. 如果属性已经存在，则覆盖; 否则，新创建用户属性
     *
     * @param account_id  账号 ID
     * @param distinct_id 访客 ID
     * @param properties  用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_set(String account_id, String distinct_id, Map<String, Object> properties)
            throws InvalidArgumentException {
        __add(distinct_id, account_id, DataType.USER_SET, properties);
    }

    /**
     * 删除用户属性
     *
     * @param account_id  账号 ID
     * @param distinct_id 访客 ID
     * @param properties  用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_unset(String account_id, String distinct_id, String... properties)
            throws InvalidArgumentException {
        if (properties == null) {
            return;
        }
        Map<String, Object> prop = new HashMap<>();
        for (String s : properties) {
            prop.put(s, 0);
        }
        __add(distinct_id, account_id, DataType.USER_UNSET, prop);
    }

    /**
     *用户的数组类型的属性追加
     *
     * @param account_id  账号 ID
     * @param distinct_id 访客 ID
     * @param properties  用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_append(String account_id, String distinct_id, Map<String, Object> properties)
            throws InvalidArgumentException {
        __add(distinct_id, account_id, DataType.USER_APPEND, properties);
    }

    /**
     * 上报事件
     *
     * @param account_id  账号 ID
     * @param distinct_id 访客ID
     * @param event_name  事件名称
     * @param properties  事件属性
     * @throws InvalidArgumentException 数据错误
     */
    public void track(String account_id, String distinct_id, String event_name, Map<String, Object> properties)
            throws InvalidArgumentException {
        if (TextUtils.isEmpty(event_name)) {
            throw new InvalidArgumentException("The event name must be provided.");
        }

        Map<String, Object> all_properties = new HashMap<String, Object>(superProperties);
        if (properties != null) {
            all_properties.putAll(properties);
        }
        __add(distinct_id, account_id, DataType.TRACK, event_name, all_properties);
    }

    private void __add(String distinct_id, String account_id, DataType type, Map<String, Object> properties)
            throws InvalidArgumentException {
        __add(distinct_id, account_id, type, null, properties);
    }

    private void __add(String distinct_id, String account_id, DataType type, String event_name, Map<String, Object> properties)
            throws InvalidArgumentException {
        if (TextUtils.isEmpty(account_id) && TextUtils.isEmpty(distinct_id)) {
            throw new InvalidArgumentException("account_id or distinct_id must be provided.");
        }

        Map<String, Object> finalProperties = (properties == null) ? new HashMap<String, Object>() : new HashMap<>(properties);
        Map<String, Object> event = new HashMap<String, Object>();

        //#uuid 只支持UUID标准格式xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        if (finalProperties.containsKey("#uuid")) {
            event.put("#uuid", finalProperties.get("#uuid"));
            finalProperties.remove("#uuid");
        }
        assertProperties(type, finalProperties);
        if (!TextUtils.isEmpty(distinct_id)) {
            event.put("#distinct_id", distinct_id);
        }

        if (!TextUtils.isEmpty(account_id)) {
            event.put("#account_id", account_id);
        }

        if (finalProperties.containsKey("#time")) {
            event.put("#time", finalProperties.get("#time"));
            finalProperties.remove("#time");
        } else {
            event.put("#time", new Date());
        }

        if (finalProperties.containsKey("#ip")) {
            event.put("#ip", finalProperties.get("#ip"));
            finalProperties.remove("#ip");
        }

        event.put("#type", type.getType());

        if (type == DataType.TRACK) {
            event.put("#event_name", event_name);
            finalProperties.put("#lib", LIB_NAME);
            finalProperties.put("#lib_version", LIB_VERSION);
        }

        event.put("properties", finalProperties);

        this.consumer.add(event);
    }

    private void assertProperties(DataType type, final Map<String, Object> properties) throws InvalidArgumentException {
        if (properties.size() == 0) return;

        if (properties.containsKey("#time")) {
            if (!(properties.get("#time") instanceof Date)) {
                throw new InvalidArgumentException("The type of #time must be Date");
            }
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
                } else if (!(value instanceof Number) && !(value instanceof Date) && !(value instanceof String) && !(value instanceof Boolean) && !(value instanceof List<?>)) {
                    throw new InvalidArgumentException("The supported data type including: Number, String, Date, Boolean,List. Invalid property: " + property.getKey());
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
            String log_directory;
            RotateMode rotateMode = RotateMode.DAILY;
            String lockFileName;
            int fileSize = 0;
            int bufferSize = 8192;

            /**
             * 创建指定日志存放路径的 LoggerConsumer 配置
             *
             * @param log_directory 日志存放路径
             */
            public Config(String log_directory) {
                this.log_directory = log_directory;
            }

            /**
             * 创建指定日志存放路径和日志大小的 LoggerConsumer 配置
             *
             * @param log_directory 日志存放路径
             * @param fileSize      日志大小, 单位 MB, 默认为无限大
             */
            public Config(String log_directory, int fileSize) {
                this.log_directory = log_directory;
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
        }

        private final String fileNamePrefix;
        private final String lockFileName;

        private final StringBuffer message_buffer = new StringBuffer();
        private final int bufferSize;
        private final int fileSize;

        private final ThreadLocal<SimpleDateFormat> df;

        private LoggerFileWriter logger_writer;

        /**
         * 创建指定配置信息的 LoggerConsumer
         *
         * @param config LoggerConsumer.Config instance.
         */
        public LoggerConsumer(final Config config) {
            this.fileNamePrefix = config.log_directory + File.separator + "log.";
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
        }

        /**
         * 创建指定日志存放目录的 LoggerConsumer. 单个日志文件大小默认为 1G.
         *
         * @param log_directory 日志存放目录
         */
        public LoggerConsumer(final String log_directory) {
            this(new Config(log_directory));
        }

        /**
         * 创建指定日志存放目录的 LoggerConsumer, 并指定单个日志文件大小.
         *
         * @param log_directory 日志目录
         * @param fileSize      单个日志文件大小限制，单位 MB
         */
        public LoggerConsumer(final String log_directory, int fileSize) {
            this(new Config(log_directory, fileSize));
        }


        @Override
        public synchronized void add(Map<String, Object> message) {
            try {
                message_buffer.append(JSON.toJSONStringWithDateFormat(message, DEFAULT_DATE_FORMAT));
                message_buffer.append("\n");
            } catch (JSONException e) {
                throw new RuntimeException("Failed to add data", e);
            }

            if (message_buffer.length() >= bufferSize) {
                this.flush();
            }
        }

        @Override
        public synchronized void flush() {
            if (message_buffer.length() == 0) {
                return;
            }

            String file_name = getFileName();
            if (logger_writer != null && !logger_writer.getFileName().equals(file_name)) {
                LoggerFileWriter.removeInstance(logger_writer);
                logger_writer = null;
            }

            if (logger_writer == null) {
                try {
                    logger_writer = LoggerFileWriter.getInstance(file_name, lockFileName);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }

            if (logger_writer.write(message_buffer)) {
                message_buffer.setLength(0);
            }
        }

        private String getFileName() {
            String resultPrefix = fileNamePrefix + df.get().format(new Date()) + "_";
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
            if (logger_writer != null) {
                LoggerFileWriter.removeInstance(logger_writer);
                logger_writer = null;
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

        private final Integer batchSize;
        private final Integer interval;
        private Long lastFlushTime = System.currentTimeMillis();
        private final Object messageLock = new Object();
        private List<Map<String, Object>> message_channel;
        private HttpService httpService;

        /**
         * 创建指定接收端地址和 APP ID 的 BatchConsumer
         *
         * @param serverUrl 接收端地址
         * @param appId     APP ID
         */
        public BatchConsumer(String serverUrl, String appId) throws URISyntaxException {
            this(serverUrl, appId, 20, null, 3);
        }

        /**
         *
         * @param serverUrl 接收端地址
         * @param appId     APP ID
         * @param config    BatchConsumer配置类
         * @throws URISyntaxException
         */
        public BatchConsumer(String serverUrl, String appId,Config config) throws URISyntaxException {
            URI uri = new URI(serverUrl);
            URI url = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(),
                    "/sync_server", uri.getQuery(), uri.getFragment());
            httpService = new HttpService(url, appId);
            this.batchSize = config.batchSize;
            this.interval = config.interval;
            this.httpService.compress = config.compress;
            this.message_channel = new LinkedList<>();
        }


        /**
         * 创建指定接收端地址和 APP ID 的 BatchConsumer，并设定 batchSize, 网络请求 timeout, 发送频次
         *
         * @param serverUrl 接收端地址
         * @param appId     APP ID
         * @param batchSize 缓存数目上线
         * @param timeout   超时时长，单位 ms
         * @param interval  发送间隔，单位秒
         */
        public BatchConsumer(String serverUrl, String appId, int batchSize, Integer timeout, int interval) throws URISyntaxException {
            this.message_channel = new LinkedList<>();
            this.batchSize = batchSize;
            this.interval = interval;
            URI uri = new URI(serverUrl);
            URI url = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(),
                    "/sync_server", uri.getQuery(), uri.getFragment());
            httpService = new HttpService(url, appId, timeout);
        }

        /**
         * BatchConsumer 的 配置类
         */
        public static class Config {
            private Integer batchSize = 20;
            private Integer interval = 3;
            private String compress="gzip";

            public Config() {
            }

            /**
             *
             * @param batchSize BatchConsumer的flush条数，缓存数目
             */
            public void setBatchSize(Integer batchSize) {
                this.batchSize = batchSize;
            }

            /**
             *
             * @param interval 发送间隔，单位秒
             */

            public void setInterval(Integer interval) {
                this.interval = interval;
            }

            /**
             *
             * @param compress BatchConsumer的压缩方式
             */
            public void setCompress(String compress) {
                this.compress = compress;
            }
        }

        @Override
        public void add(Map<String, Object> message) {
            synchronized (messageLock) {
                message_channel.add(message);
                Long nowTime = System.currentTimeMillis();
                if (message_channel.size() >= batchSize || (nowTime - lastFlushTime >= interval * 1000)) {
                    flush();
                }
            }
        }

        @Override
        public void flush() {
            synchronized (messageLock) {
                boolean deleteData = true;
                try {
                    List<Map<String, Object>> messageList;
                    if (message_channel.size() > batchSize) {
                        messageList = message_channel.subList(0, batchSize);
                    } else if (message_channel.size() == 0) {
                        return;
                    } else {
                        messageList = message_channel;
                    }

                    String data = JSON.toJSONStringWithDateFormat(messageList, DEFAULT_DATE_FORMAT);
                    httpService.send(data);

                } catch (JSONException e) {
                    throw new RuntimeException("Failed to for json operations", e);
                } catch (InvalidArgumentException e) {
                    throw new RuntimeException("Invalid parameter", e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (HttpService.ServiceUnavailableException e) {
                    deleteData = false;
                    e.printStackTrace();
                } finally {
                    if (deleteData) {
                        if (message_channel.size() > batchSize) {
                            message_channel = message_channel.subList(batchSize, message_channel.size());
                        } else {
                            message_channel.clear();
                        }
                    }
                    lastFlushTime = System.currentTimeMillis();
                }
            }
        }

        @Override
        public void close() {
            while (message_channel.size() > 0) {
                flush();
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
                httpService.send(data);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void flush() {

        }

        @Override
        public void close() {

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
        private CloseableHttpClient httpClient = null;

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
            this.httpClient = HttpRequestUtil.getHttpClient();
            this.serverUri = server_uri;
            this.appId = appId;
        }

        public synchronized void send(final String data) throws ServiceUnavailableException, InvalidArgumentException, IOException {
            HttpPost httpPost = new HttpPost(serverUri);
            HttpEntity params = (consumeMode == ConsumeMode.BATCH) ? getBatchHttpEntity(data) : getDebugHttpEntity(data);
            httpPost.setEntity(params);
            httpPost.addHeader("appid", this.appId);
            httpPost.addHeader("user-agent", "java_sdk_" + LIB_VERSION);
            httpPost.addHeader("version", LIB_VERSION);
            httpPost.addHeader("compress", compress);
            if (this.connectTimeout != null) {
                RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(connectTimeout + 10000).setConnectTimeout(connectTimeout).build();
                httpPost.setConfig(requestConfig);
            }
            try (CloseableHttpResponse response = this.httpClient.execute(httpPost)) {
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode < 200 || statusCode > 300) {
                    throw new ServiceUnavailableException("Cannot post message to " + this.serverUri);
                }
                String result = EntityUtils.toString(response.getEntity(), "UTF-8");
                JSONObject resultJson = JSONObject.parseObject(result);
                checkingRetCode(resultJson);
            } catch (IOException e) {
                throw new ServiceUnavailableException("Cannot post message to " + this.serverUri);
            } finally {
                httpPost.releaseConnection();
            }
        }

        UrlEncodedFormEntity getDebugHttpEntity(final String data) throws IOException {
            List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();
            nameValuePairs.add(new BasicNameValuePair("source", "server"));
            nameValuePairs.add(new BasicNameValuePair("appid", this.appId));
            nameValuePairs.add(new BasicNameValuePair("data", data));
            if (!this.writeData) {
                nameValuePairs.add(new BasicNameValuePair("dryRun", String.valueOf(1)));
            }
            return new UrlEncodedFormEntity(nameValuePairs, "UTF-8");
        }

        HttpEntity getBatchHttpEntity(final String data) throws IOException, InvalidArgumentException {
            byte[] dataCompressed = null;
            byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
            if ("gzip".equalsIgnoreCase(this.compress)) {
                dataCompressed = gzipCompress(dataBytes);
            } else if ("lzo".equalsIgnoreCase(this.compress)) {
                dataCompressed = lzoCompress(dataBytes);
            } else if ("lz4".equalsIgnoreCase(this.compress)) {
                dataCompressed = lz4Compress(dataBytes);
            } else if ("none".equalsIgnoreCase(this.compress)) {
                dataCompressed = dataBytes;
            }else {
                throw new InvalidArgumentException("compress input error.");
            }
            return new ByteArrayEntity(dataCompressed);
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

        private void checkingRetCode(JSONObject resultJson) throws InvalidArgumentException {
            if (this.consumeMode == ConsumeMode.DEBUG) {
                if (resultJson.getInteger("errorLevel") != 0) {
                    throw new InvalidArgumentException(resultJson.toJSONString());
                }
            } else if (this.consumeMode == ConsumeMode.BATCH) {
                int retCode = resultJson.getInteger("code");
                if (retCode != 0) {
                    if (retCode == -1) {
                        throw new InvalidArgumentException(resultJson.containsKey("msg") ? resultJson.getString("msg") : "invalid data format");
                    } else if (retCode == -2) {
                        throw new InvalidArgumentException(resultJson.containsKey("msg") ? resultJson.getString("msg") : "APP ID doesn't exist");
                    } else if (retCode == -3) {
                        throw new InvalidArgumentException(resultJson.containsKey("msg") ? resultJson.getString("msg") : "invalid ip transmission");
                    } else {
                        throw new RuntimeException("Unexpected response return code: " + retCode);
                    }
                }
            }
        }

        class ServiceUnavailableException extends Exception {
            ServiceUnavailableException(String message) {
                super(message);
            }
        }

        @Override
        public void close() {

        }

    }
}
