package cn.thinkingdata.analytics;

import cn.thinkingdata.analytics.inter.ITDConsumer;
import cn.thinkingdata.analytics.util.TDCommonUtil;
import cn.thinkingdata.analytics.util.TDLogger;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import static cn.thinkingdata.analytics.TDConstData.DEFAULT_DATE_FORMAT;
/**
 * Write data to file, it works with LogBus2
 */
public class TDLoggerConsumer implements ITDConsumer {
    /**
     * File rotate mode
     */
    public enum RotateMode {
        /**
         * in days
         */
        DAILY,
        /**
         * in hours
         */
        HOURLY
    }

    /**
     * LoggerConsumer config
     */
    public static class Config {
        String logDirectory;                            // directory of log file
        RotateMode rotateMode = RotateMode.DAILY;       // file rotate mode, in days default
        String lockFileName;                            // lock file
        String fileNamePrefix;                          // prefix of log file
        int interval = 0;                               // auto flush interval (second)
        int fileSize = 0;                               // max size of single log file (MByte)
        int bufferSize = 8192;                          // buffer size (unit: byte)
        boolean autoFlush = false;                      // is enable auto flush or not


        /**
         * init LoggerConsumer config
         *
         * @param logDirectory directory of log file
         */
        public Config(String logDirectory) {
            this(logDirectory, 0);
        }

        /**
         * init LoggerConsumer config
         *
         * @param logDirectory directory of log file
         * @param fileSize     max size of single log file (MByte), default infinite
         */
        public Config(String logDirectory, int fileSize) {
            this.logDirectory = logDirectory;
            this.fileSize = fileSize;
        }

        /**
         * set file rotate mode
         *
         * @param rotateMode remote mode
         */
        public void setRotateMode(RotateMode rotateMode) {
            this.rotateMode = rotateMode;
        }

        /**
         * set file size
         *
         * @param fileSize file size (unit: Mb)
         */
        public void setFileSize(int fileSize) {
            this.fileSize = fileSize;
        }

        /**
         * set lock file
         * @param lockFileName lock file name
         */
        public void setLockFile(String lockFileName) {
            this.lockFileName = lockFileName;
        }

        /**
         * set buffer size
         *
         * @param bufferSize buffer size (unit: byte).
         */
        public void setBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
        }

        /**
         * prefix of file
         *
         * @param fileNamePrefix prefix
         */
        public void setFilenamePrefix(String fileNamePrefix) {
            this.fileNamePrefix = fileNamePrefix;
        }

        /**
         * is auto flush or not
         *
         * @param autoFlush auto flush
         */
        public void setAutoFlush(boolean autoFlush) {
            this.autoFlush = autoFlush;
        }

        /**
         * auto flush interval
         *
         * @param interval interval
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

    private boolean isClose;

    /**
     * init LoggerConsumer with config
     *
     * @param config LoggerConsumer.Config instance.
     */
    public TDLoggerConsumer(final Config config) {
        if (config.logDirectory == null || config.logDirectory.isEmpty()) {
            throw new RuntimeException("directory for file is not be empty!");
        }
        TDLogger.println("LogConsumer Model,LogDirectory="+config.logDirectory);
        File dir = new File(config.logDirectory);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        if (!dir.isDirectory()) {
            throw new RuntimeException("path of file is not directory" + config.logDirectory);
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
     * init LoggerConsumer
     *
     * @param logDirectory directory of file
     */
    public TDLoggerConsumer(final String logDirectory) {
        this(new Config(logDirectory));
    }

    /**
     * init LoggerConsumer
     *
     * @param logDirectory directory of file
     * @param fileSize     max size of single log file (MByte), default infinite 
     */
    public TDLoggerConsumer(final String logDirectory, int fileSize) {
        this(new Config(logDirectory, fileSize));
    }

    @Override
    public synchronized void add(Map<String, Object> message) {
        if (isClose) {
            TDLogger.println("SDK is already closed");
            return;
        }
        try {
            String formatMsg = JSON.toJSONString(message, DEFAULT_DATE_FORMAT, TDCommonUtil.fastJsonSerializerFeature());
            messageBuffer.append(formatMsg);
            messageBuffer.append("\n");
            TDLogger.println("Enqueue data: " + formatMsg);
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
            TDLogger.print("flush data: " + messageBuffer);
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
        isClose = true;
    }

    private static class LoggerFileWriter {
        private final String fileName;
        private final FileOutputStream outputStream;
        private final FileOutputStream lockStream;
        private int refCount;

        private static final Map<String, LoggerFileWriter> instances = new HashMap<String, LoggerFileWriter>();

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
                    TDLogger.println(e.getLocalizedMessage());
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

