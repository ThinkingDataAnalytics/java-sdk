package cn.thinkingdata.analytics.exception;

public class InvalidArgumentException extends Exception {

    private static final long serialVersionUID = 1L;

    public InvalidArgumentException(String message) {
        super(message);
    }

    public InvalidArgumentException(Throwable error) {
        super(error);
    }
}
