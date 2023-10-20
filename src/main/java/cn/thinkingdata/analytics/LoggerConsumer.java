package cn.thinkingdata.analytics;

/**
 * @deprecated please use TDLoggerConsumer instead
 */
@Deprecated
public class LoggerConsumer extends TDLoggerConsumer {

    /**
     * init LoggerConsumer with config
     *
     * @param config LoggerConsumer.Config instance.
     */
    public LoggerConsumer(final Config config) {
        super(config);
    }

    /**
     * init LoggerConsumer
     *
     * @param logDirectory directory of file
     */
    public LoggerConsumer(final String logDirectory) {
        this(new Config(logDirectory));
    }

    /**
     * init LoggerConsumer
     *
     * @param logDirectory directory of file
     * @param fileSize     max size of single log file (MByte), default infinite
     */
    public LoggerConsumer(final String logDirectory, int fileSize) {
        this(new Config(logDirectory, fileSize));
    }
}
