package cn.thinkingdata.tga.javasdk.util;
import cn.thinkingdata.tga.javasdk.exception.InvalidArgumentException;
import org.apache.http.util.TextUtils;

import java.util.Map;
import static cn.thinkingdata.tga.javasdk.TAConstData.*;
public class TAPropertyUtil {
    /**
     * 判断properties是否符合要求，默认只检测key,requireValueType传入null即可
     * @param properties 需要检测的properties
     * @param type 数据类型
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
     * 判断属性key是否满足要求
     * @param key 属性key的值
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
     * 属性组合，按照sourceProperties顺序进行覆盖，排在最后面的优先级最高
     * @param desProperties 组合的目标属性实体
     * @param sourceProperties 即将组合的属性实体
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
     * 某些特殊的属性，数据体内部会做一些调整
     * @param data 用户属性或者事件构建的data
     * @param properties 用户或者事件属性
     * @param propertyKeys 某些特殊的属性key
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
