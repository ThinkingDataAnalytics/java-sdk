package cn.thinkingdata.analytics.util;

import cn.thinkingdata.analytics.exception.InvalidArgumentException;
import com.alibaba.fastjson.serializer.SerializerFeature;
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

    public static int fastJsonSerializerFeature() {
        int features = 0;
        features = features | SerializerFeature.QuoteFieldNames.getMask();
        features |= SerializerFeature.SkipTransientField.getMask();
        features |= SerializerFeature.WriteEnumUsingName.getMask();
        features |= SerializerFeature.SortField.getMask();
        return features;
    }
}
