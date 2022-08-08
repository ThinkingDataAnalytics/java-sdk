package cn.thinkingdata.tga.javasdk;

import cn.thinkingdata.tga.javasdk.inter.Consumer;
import cn.thinkingdata.tga.javasdk.util.TALogger;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import static cn.thinkingdata.tga.javasdk.TAConstData.DEFAULT_DATE_FORMAT;
/**
 * LoggerConsumer 批量实时写本地文件，文件以天为分隔，需要搭配 LogBus 进行上传. 建议使用.
 */
public class LoggerConsumer implements Consumer {
    /**
     * 日志切分模式
     */
    public enum RotateMode {
        DAILY,//按天切分
        HOURLY//按小时切分
    }

    /**
     * LoggerConsumer 的配置信息
     */
    public static class Config {
        String logDirectory;                            // 日志存放路径
        RotateMode rotateMode = RotateMode.DAILY;       // 日志切分模式，默认以天为单位
        String lockFileName;                            //
        String fileNamePrefix;                          // 日志文件名前缀
        int interval = 0;                               // 日志上传间隔
        int fileSize = 0;                               // 单个日志文件的最大大小
        int bufferSize = 8192;                          // 缓冲区容量
        boolean autoFlush = false;                      // 是否开启定时器




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
        TALogger.print("LogConsumer Model,LogDirectory="+config.logDirectory);
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
            String formatMsg = JSON.toJSONStringWithDateFormat(message, DEFAULT_DATE_FORMAT);
            messageBuffer.append(formatMsg);
            TALogger.print("collect data="+formatMsg);
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
            TALogger.print("flush data="+messageBuffer);
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
                    TALogger.print(e.getLocalizedMessage());
                    throw new RuntimeException("failed to write file.", e);
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

