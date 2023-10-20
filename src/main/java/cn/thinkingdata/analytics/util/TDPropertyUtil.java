package cn.thinkingdata.analytics.util;
import cn.thinkingdata.analytics.exception.InvalidArgumentException;
import org.apache.http.util.TextUtils;

import java.util.Map;
import static cn.thinkingdata.analytics.TDConstData.*;
public class TDPropertyUtil {
    /**
     * check properties
     * @param properties properties
     * @param type event type
     * */
    public static  void assertProperties(final Map<String, Object> properties,final DataType type) throws InvalidArgumentException {
        if (properties.size() == 0) {
            return;
        }
        for (Map.Entry<String, Object> property : properties.entrySet()) {
            Object value = property.getValue();
            if (null == value) {
                continue;
            }
            if (! isValidKey(property.getKey())) {
                throw new InvalidArgumentException("Invalid key format: " + property.getKey());
            }
            if(type == DataType.USER_ADD)
            {
                if ( ! (value instanceof  Number))
                {
                    throw new InvalidArgumentException("Only Number is allowed,Invalid property value " + value);
                }
            }
        }
    }
    /**
     * check property name
     * @param key key
     * */
    public static boolean isValidKey(String key)
    {
        if(TextUtils.isEmpty(key))
        {
            return  false;
        }
        return KEY_PATTERN.matcher(key).matches();
    }

    /**
     * combine properties by sort. The latter has the highest priority
     * @param desProperties target
     * @param sourceProperties source
     * */
    @SafeVarargs
    public static void mergeProperties(Map<String,Object> desProperties,Map<String,Object> ... sourceProperties) throws InvalidArgumentException {
        for(Map<String,Object> sourceProperty:sourceProperties)
        {
            if(sourceProperty != null)
            {
                desProperties.putAll(sourceProperty);
            }
        }
    }
    /**
     * Move special properties
     * @param data target
     * @param properties source
     * @param propertyKeys properties key
     * */
    public  static  void moveProperty(Map<String,Object> data,Map<String,Object> properties,String... propertyKeys)
    {
        for (String propertyKey:propertyKeys)
        {
            if(properties.containsKey(propertyKey))
            {
                data.put(propertyKey, properties.get(propertyKey));
                properties.remove(propertyKey);
            }
        }
    }
}
