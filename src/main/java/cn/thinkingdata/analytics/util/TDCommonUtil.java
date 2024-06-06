package cn.thinkingdata.analytics.util;

import cn.thinkingdata.analytics.exception.InvalidArgumentException;
import com.alibaba.fastjson2.JSONWriter;
import org.apache.http.util.TextUtils;

import java.util.Map;

public class TDCommonUtil {
    public static  void buildData(Map<String,Object> data, String key, Object value)
    {
        if(!TextUtils.isEmpty(key) && value != null)
        {
            data.put(key,value);
        }
    }
    /**
     * is value empty or not
     * @param value data
     * @param exceptionMsg exception
     * */
    public static  void throwEmptyException (String value,String exceptionMsg) throws InvalidArgumentException {
        if(TextUtils.isEmpty(value))
            throw new InvalidArgumentException(exceptionMsg);
    }

    public static JSONWriter.Feature[] fastJsonSerializerFeature() {
        return new JSONWriter.Feature[]{
                JSONWriter.Feature.WriteEnumsUsingName,
                JSONWriter.Feature.MapSortField
        };
    }
}
