package cn.thinkingdata.tga.javasdk.exception;

/**
 * @author Sun Zeyu
 * @date 2021/3/13 1:49 下午
 */
public class IllegalDataException extends RuntimeException {

    public IllegalDataException(String message) {
        super(message);
    }

    public IllegalDataException(String message, Throwable t) {
        super(message, t);
    }
}
