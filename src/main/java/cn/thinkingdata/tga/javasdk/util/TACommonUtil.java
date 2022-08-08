package cn.thinkingdata.tga.javasdk.util;

import cn.thinkingdata.tga.javasdk.exception.InvalidArgumentException;
import org.apache.http.util.TextUtils;

import java.util.Map;

public class TACommonUtil {
    public static  void buildData(Map<String,Object> data, String key, Object value)
    {
        if(!TextUtils.isEmpty(key) && value != null)
        {
            data.put(key,value);
        }
    }
    /**
     * 判断value是否为空,如果是空，抛出异常
     * @param value 需要判断的数据
     * @param exceptionMsg 异常抛出时，提升的异常信息
     * */
    public static  void throwEmptyException (String value,String exceptionMsg) throws InvalidArgumentException {
        if(TextUtils.isEmpty(value))
            throw new InvalidArgumentException(exceptionMsg);
    }
}
