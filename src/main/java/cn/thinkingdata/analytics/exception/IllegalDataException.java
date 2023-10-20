package cn.thinkingdata.analytics.exception;

/**
 * @author Sun Zeyu
 */
public class IllegalDataException extends RuntimeException {

    public IllegalDataException(String message) {
        super(message);
    }

    public IllegalDataException(String message, Throwable t) {
        super(message, t);
    }
}
