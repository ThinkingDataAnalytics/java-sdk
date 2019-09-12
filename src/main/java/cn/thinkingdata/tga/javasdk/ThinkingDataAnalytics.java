package cn.thinkingdata.tga.javasdk;

import cn.thinkingdata.tga.javasdk.exception.InvalidArgumentException;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
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

    private final Consumer consumer ;
    private final Map<String,Object> superProperties;

    private final static String LIB_VERSION = "1.2.0";
    private final static String LIB_NAME = "tga_java_sdk";

    private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private final static Pattern KEY_PATTERN = Pattern.compile("^(#[a-z][a-z0-9_]{0,49})|([a-z][a-z0-9_]{0,50})$",Pattern.CASE_INSENSITIVE);

    /**
     * 构造函数.
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
        USER_DEL("user_del");

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
     * @param account_id 账号 ID
     * @param distinct_id 访客 ID
     * @throws InvalidArgumentException 数据错误
     */
    public void user_del(String account_id, String distinct_id)
            throws InvalidArgumentException {
        __add(distinct_id, account_id, DataType.USER_DEL, null);
    }

    /**
     * 用户属性修改，只支持数字属性增加的接口
     * @param account_id 账号 ID
     * @param distinct_id 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_add(String account_id, String distinct_id, Map<String,Object> properties)
            throws InvalidArgumentException {
        __add(distinct_id, account_id, DataType.USER_ADD, properties);
    }

    /**
     * 设置用户属性. 如果该属性已经存在，该操作无效.
     * @param account_id 账号 ID
     * @param distinct_id 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_setOnce(String account_id, String distinct_id, Map<String,Object> properties)
            throws InvalidArgumentException {
        __add(distinct_id, account_id, DataType.USER_SET_ONCE, properties);
    }

    /**
     * 设置用户属性. 如果属性已经存在，则覆盖; 否则，新创建用户属性
     * @param account_id 账号 ID
     * @param distinct_id 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException	数据错误
     */
    public void user_set(String account_id, String distinct_id, Map<String,Object> properties)
            throws InvalidArgumentException {
        __add(distinct_id, account_id, DataType.USER_SET, properties);
    }

    /**
     * 上报事件
     * @param account_id 账号 ID
     * @param distinct_id 访客ID
     * @param event_name 事件名称
     * @param properties 事件属性
     * @throws InvalidArgumentException	数据错误
     */
    public void track(String account_id, String distinct_id, String event_name, Map<String,Object> properties)
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

    private void __add(String distinct_id, String account_id, DataType type, Map<String,Object> properties)
            throws InvalidArgumentException {
        __add(distinct_id, account_id, type,null, properties);
    }

    private void __add(String distinct_id, String account_id, DataType type, String event_name, Map<String, Object> properties)
            throws InvalidArgumentException {
        if (TextUtils.isEmpty(account_id) && TextUtils.isEmpty(distinct_id)) {
            throw new InvalidArgumentException("account_id or distinct_id must be provided.");
        }

        assertProperties(type, properties);

        Map<String, Object> finalProperties = (properties == null) ? new HashMap<String, Object>() : new HashMap<>(properties);
        Map<String,Object> event = new HashMap<String,Object>();

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
        } else {
            event.put("#ip", "");
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
        if (null == properties) return;

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
                } else if (!(value instanceof Number) && !(value instanceof Date) && !(value instanceof String) && !(value instanceof Boolean)) {
                    throw new InvalidArgumentException("The supported data type including: Number, String, Date, Boolean. Invalid property: " + property.getKey());
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
            /** 按日切分 */
            DAILY,
            /** 按小时切分 */
            HOURLY
        }

        /**
         * LoggerConsumer 的配置信息
         */
        public static class Config {
            String log_directory;
            RotateMode rotateMode = RotateMode.DAILY;
            String lockFileName;
            int maxFileSize = 1024;
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
             * @param maxFileSize 日志大小, 单位 MB, 默认为 1024 MB
             */
            public Config(String log_directory, int maxFileSize) {
                this.log_directory = log_directory;
                this.maxFileSize = maxFileSize;
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
             * @param maxFileSize 日志大小，单位 MB
             */
            public void setFileSize(int maxFileSize) {
                this.maxFileSize = maxFileSize;
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
        private final int maxFileSize;

        private final ThreadLocal<SimpleDateFormat> df;

        private LoggerFileWriter logger_writer;

        /**
         * 创建指定配置信息的 LoggerConsumer
         *
         * @param config LoggerConsumer.Config instance.
         */
        public LoggerConsumer(final Config config) {
            this.fileNamePrefix = config.log_directory + File.separator + "log.";
            this.maxFileSize = config.maxFileSize;
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
         * @param maxFileSize 单个日志文件大小限制，单位 MB
         */
        public LoggerConsumer(final String log_directory, int maxFileSize) {
            this(new Config(log_directory, maxFileSize));
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

            File target = new File(result);
            while (target.exists()) {
                if ((target.length() / (1024 * 1024)) < maxFileSize) {
                    break;
                }
                result = resultPrefix + (++count);
                target = new File(result);
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

        private Integer batchSize = 20;
        private Integer interval = 3;
        private Long lastFlushTime = System.currentTimeMillis();
        private final Object messageLock = new Object();
        private List<Map<String, Object>> message_channel;
        private HttpService httpService;

        /**
         * 创建指定接收端地址和 APP ID 的 BatchConsumer
         *
         * @param serverUrl 接收端地址
         * @param appId APP ID
         */
        public BatchConsumer(String serverUrl, String appId) throws URISyntaxException {
            this.message_channel = new CopyOnWriteArrayList<>();
            httpService = new HttpService(new URI(serverUrl), appId);
        }

        /**
         * 创建指定接收端地址和 APP ID 的 BatchConsumer，并设定 batchSize, 网络请求 timeout, 发送频次
         *
         * @param serverUrl 接收端地址
         * @param appId APP ID
         * @param batchSize 缓存数目上线
         * @param timeout 超时时长，单位 ms
         * @param interval 发送间隔，单位秒
         */
        public BatchConsumer(String serverUrl, String appId, int batchSize, int timeout, int interval) throws URISyntaxException {
            this.message_channel = new CopyOnWriteArrayList<>();
            this.batchSize = batchSize;
            this.interval = interval;
            httpService = new HttpService(new URI(serverUrl), appId, timeout);
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
                    } else {
                        messageList = message_channel;
                    }

                    String data = JSON.toJSONStringWithDateFormat(messageList, DEFAULT_DATE_FORMAT);
                    httpService.send(data);

                } catch (JSONException e) {
                    throw new RuntimeException("Failed to for json operations", e);
                } catch (InvalidArgumentException e) {
                    throw new RuntimeException("Invalid parameter", e);
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
            while(message_channel.size() > 0) {
                flush();
            }
        }
    }

    public static class DebugConsumer implements Consumer {
        HttpService httpService;

        /**
         * 创建指定接收端地址和 APP ID 的 DebugConsumer
         * @param serverUrl 接收端地址
         * @param appId APP ID
         */
        public DebugConsumer(String serverUrl, String appId) throws URISyntaxException {
            URI uri = new URI(serverUrl);
            URI restfulUri = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(),
                    "/sync_data", uri.getQuery(), uri.getFragment());

            httpService = new HttpService(restfulUri, appId);
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

    private static class HttpService {
        public enum CompressMode {
            GZIP, DEBUG
        }

        private CompressMode compressMode = CompressMode.GZIP;

        private final URI serverUri;
        private final String appId;
        private Integer connectTimeout = 30000;

        void setDebugMode() {
            this.compressMode = CompressMode.DEBUG;
        }

        private HttpService(URI server_uri, String appId, Integer timeout) {
            this(server_uri, appId);
            this.connectTimeout = timeout;
        }

        private HttpService(URI server_uri, String appId) {
            this.serverUri = server_uri;
            this.appId = appId;
        }

        public void send(final String data) throws ServiceUnavailableException, InvalidArgumentException {
            CloseableHttpResponse response = null;

            try (CloseableHttpClient httpclient = HttpClients.custom().build()) {
                RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(connectTimeout + 10000).setConnectTimeout(30000).build();
                HttpPost httpPost = new HttpPost(serverUri);

                HttpEntity params = (compressMode == CompressMode.GZIP) ? getGzipStringEntity(data) : getDebugHttpEntity(data);
                httpPost.setEntity(params);

                httpPost.addHeader("appid", this.appId);
                httpPost.addHeader("user-agent", "java_sdk_" + LIB_VERSION);
                httpPost.addHeader("version", LIB_VERSION);
                httpPost.setConfig(requestConfig);

                response = httpclient.execute(httpPost);

                int statusCode = response.getStatusLine().getStatusCode();

                if (statusCode < 200 || statusCode > 300) {
                    throw new ServiceUnavailableException("Cannot post message to " + this.serverUri);
                }

                String result = EntityUtils.toString(response.getEntity(), "UTF-8");
                JSONObject resultJson = JSONObject.parseObject(result);

                int retCode = resultJson.getInteger("code");
                if (retCode != 0 ) {
                    if (retCode == -1) {
                        throw new InvalidArgumentException(resultJson.containsKey("msg") ? resultJson.getString("msg") : "invalid data format");
                    } else if (retCode == -2) {
                        throw new InvalidArgumentException(resultJson.containsKey("msg") ? resultJson.getString("msg") : "APP ID doesn't exist");
                    } else if (retCode == -3) {
                        throw new InvalidArgumentException(resultJson.containsKey("msg") ? resultJson.getString("msg") : "invalid ip transmission");
                    } else {
                        throw new RuntimeException("Unexpected response return code: "  + retCode);
                    }
                }
            } catch (IOException e) {
                throw new ServiceUnavailableException("Cannot post message to " + this.serverUri);
            } finally {
                try {
                    if (response != null) {
                        response.close();
                    }
                } catch (IOException e) {
                    // ignore
                }
            }
        }


        UrlEncodedFormEntity getDebugHttpEntity(final String data) throws IOException {
            List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();
            nameValuePairs.add(new BasicNameValuePair("debug", String.valueOf(1)));
            nameValuePairs.add(new BasicNameValuePair("appid", this.appId));
            nameValuePairs.add(new BasicNameValuePair("data", data));
            return new UrlEncodedFormEntity(nameValuePairs, "UTF-8");
        }

        StringEntity getGzipStringEntity(final String data) {
            ByteArrayOutputStream byteArrayBuffer = new ByteArrayOutputStream();
            try {
                GZIPOutputStream var = new GZIPOutputStream(byteArrayBuffer);
                var.write(data.getBytes(StandardCharsets.UTF_8));
                var.close();
            } catch(IOException e) {
                return null;
            }

            return new StringEntity(new String(Base64.encodeBase64(byteArrayBuffer.toByteArray())), "UTF-8");
        }

        class ServiceUnavailableException extends Exception {
            ServiceUnavailableException(String message) {
                super(message);
            }
        }
    }
}
