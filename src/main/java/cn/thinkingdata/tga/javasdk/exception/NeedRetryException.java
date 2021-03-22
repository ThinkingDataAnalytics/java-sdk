package cn.thinkingdata.tga.javasdk.exception;

/**
 * @author Sun Zeyu
 * @date 2021/3/13 1:46 下午
 */
public class NeedRetryException extends RuntimeException {

    public NeedRetryException(String message) {
        super(message);
    }

    public NeedRetryException(String message, Throwable t) {
        super(message, t);
    }
}
