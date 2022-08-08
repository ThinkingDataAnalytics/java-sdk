package cn.thinkingdata.tga.javasdk;

import cn.thinkingdata.tga.javasdk.exception.InvalidArgumentException;
import cn.thinkingdata.tga.javasdk.inter.Consumer;
import cn.thinkingdata.tga.javasdk.inter.DynamicSuperPropertiesTracker;
import cn.thinkingdata.tga.javasdk.inter.ITALogger;
import cn.thinkingdata.tga.javasdk.inter.IThinkingDataAnalytics;
import cn.thinkingdata.tga.javasdk.util.TACommonUtil;
import cn.thinkingdata.tga.javasdk.util.TALogger;
import cn.thinkingdata.tga.javasdk.util.TAPropertyUtil;
import org.apache.http.util.TextUtils;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import static cn.thinkingdata.tga.javasdk.TAConstData.*;
public class ThinkingDataAnalytics implements IThinkingDataAnalytics {
    private final Consumer consumer;
    private final Map<String, Object> superProperties; // 公共属性
    private final boolean enableUUID;
    boolean isStrictModel = false;                    //默认为false，错误的数据会上报的TA后台；true的情况下，不正确的数据，不会上报到TA后台
    // 动态公共属性回调
    private DynamicSuperPropertiesTracker dynamicSuperProperties = null;
    /**
     * 构造函数.
     * @param consumer BatchConsumer, LoggerConsumer,DebugConsumer
     */
    public ThinkingDataAnalytics(final Consumer consumer) {
        this(consumer,false);
    }
    public ThinkingDataAnalytics(final Consumer consumer, final boolean enableUUID) {
        this(consumer,enableUUID,consumer instanceof  DebugConsumer ? true :false);
    }
    public ThinkingDataAnalytics(final Consumer consumer, final boolean enableUUID,final  boolean isStrictModel) {
        this.consumer = consumer;
        this.enableUUID = enableUUID;
        this.superProperties = new ConcurrentHashMap<>();
        this.isStrictModel = isStrictModel;
    }
    /**
     * 删除用户，此操作不可逆
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @throws InvalidArgumentException 数据错误
     */
    public void user_del(String accountId, String distinctId)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.USER_DEL, null);
    }
    /**
     * 用户属性修改，只支持数字属性增加的接口
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_add(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.USER_ADD, properties);
    }

    /**
     * 设置用户属性. 如果该属性已经存在，该操作无效.
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_setOnce(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.USER_SET_ONCE, properties);
    }

    /**
     * 设置用户属性. 如果属性已经存在，则覆盖; 否则，新创建用户属性
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_set(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.USER_SET, properties);
    }

    /**
     * 删除用户指定的属性
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_unset(String accountId, String distinctId, String... properties)
            throws InvalidArgumentException {
        if (properties == null) {
            return;
        }
        Map<String, Object> prop = new HashMap<>();
        for (String s : properties) {
            prop.put(s, 0);
        }
        add(distinctId, accountId, DataType.USER_UNSET, prop);
    }

    /**
     * 用户的数组类型的属性追加
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_append(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.USER_APPEND, properties);
    }

    /**
     * 用户的数组类型的属性去重追加
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_uniqAppend(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.USER_UNIQ_APPEND, properties);
    }

    /**
     * 上报事件
     *
     * @param accountId  账号 ID
     * @param distinctId 访客ID
     * @param eventName  事件名称
     * @param properties 事件属性
     * @throws InvalidArgumentException 数据错误
     */
    public void track(String accountId, String distinctId, String eventName, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.TRACK, eventName, null, properties);
    }
    /**
     * 首次事件
     * @param accountId     账号ID
     * @param distinctId    访客ID
     * @param eventName     事件名称
     * @param properties    事件属性
     *                      (必须要在properties中设置#first_check_id字段，该字段是校验首次事件的标识ID)
     * @throws InvalidArgumentException   数据错误
     */
    public void track_first(String accountId, String distinctId, String eventName, Map<String, Object> properties)
            throws InvalidArgumentException {
        if (properties.containsKey("#first_check_id")) {
            track(accountId, distinctId, eventName, properties);
        }else  {
            throw new InvalidArgumentException("#first_check_id key must set");
        }
    }

    /**
     * @param accountId  账号 ID
     * @param distinctId 访客ID
     * @param eventName  事件名称
     * @param eventId    事件ID
     * @param properties 事件属性
     * @throws InvalidArgumentException 数据错误
     */
    public void track_update(String accountId, String distinctId, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.TRACK_UPDATE, eventName, eventId,properties);
    }

    /**
     * @param accountId  账号 ID
     * @param distinctId 访客ID
     * @param eventName  事件ID
     * @param eventId    事件ID
     * @param properties 事件属性
     * @throws InvalidArgumentException 数据错误
     */
    public void track_overwrite(String accountId, String distinctId, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.TRACK_OVERWRITE, eventName, eventId,properties);
    }

    /**
     * 清除公共事件属性
     */
    public void clearSuperProperties() {
        this.superProperties.clear();
    }

    /**
     * 设置公共事件属性. 公共事件属性会添加到每个事件的属性中上报
     *
     * @param properties 公共属性
     */
    public void setSuperProperties(Map<String, Object> properties) {
        this.superProperties.putAll(properties);
    }

    /**
     * 设置动态公共属性，即此处设置的公共属性会在上报时获取值
     * 建议此回调方法中不要加入大量计算操作代码
     * @param dynamicSuperPropertiesTracker 动态公共属性
     */
    public void setDynamicSuperPropertiesTracker(DynamicSuperPropertiesTracker dynamicSuperPropertiesTracker) {
        dynamicSuperProperties = dynamicSuperPropertiesTracker;
    }

    /**
     * 立即上报数据到接收端
     */
    public void flush() {
        this.consumer.flush();
    }

    /**
     * 关闭并退出 sdk 所有线程，停止前会清空所有本地数据
     */
    public void close() {
        this.consumer.close();
    }

    private void add(String distinctId, String accountId, DataType type, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, type, null, null, properties);
    }

    private void add(String distinctId, String accountId, DataType type, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException {
        if(isStrictModel && TextUtils.isEmpty(accountId) && TextUtils.isEmpty(distinctId))
            throw new InvalidArgumentException("accountId or distinctId must be provided.");
        Map<String, Object> copyProperties =  properties!=null ? new ConcurrentHashMap<>(properties) : new ConcurrentHashMap<String, Object>();
        Map<String, Object> desProperties =  new HashMap<>();
        Map<String, Object> event = new HashMap<>();
        TACommonUtil.buildData(event,"#distinct_id", distinctId);
        TACommonUtil.buildData(event,"#account_id", accountId);
        event.put("#time", new Date());
        event.put("#type", type.getType());
        //#uuid 只支持UUID标准格式
        if(!event.containsKey("#uuid") && enableUUID)
        {
            event.put("#uuid", UUID.randomUUID().toString());
        }
        //特殊属性，内部会做一些数据结构转化
        TAPropertyUtil.moveProperty(event,copyProperties,"#uuid","#time","#ip","#app_id","#first_check_id","#transaction_property","#import_tool_id");
        if (type.getType().contains("track")) {
            if(isStrictModel)
                TACommonUtil.throwEmptyException(eventName,"The event name must be provided.");
            if (type.getType().contains("track_"))
            {
                if(isStrictModel)
                    TACommonUtil.throwEmptyException(eventId,"The event id must be provided.");
                TACommonUtil.buildData(event,"#event_id", eventId);
            }
            TAPropertyUtil.mergeProperties(desProperties,superProperties,dynamicSuperProperties!=null ? dynamicSuperProperties.getDynamicSuperProperties():null,copyProperties);
            event.put("#event_name", eventName);
            desProperties.put("#lib", LIB_NAME);
            desProperties.put("#lib_version", LIB_VERSION);
        }else
        {
            TAPropertyUtil.mergeProperties(desProperties,copyProperties);
        }
        if(isStrictModel)
            TAPropertyUtil.assertProperties(desProperties,type);
        event.put("properties",desProperties);
        this.consumer.add(event);
    }
    //是否打印SDK日志，Debug模式下默认开启
    public static  void enableLog(boolean isPrintLog)
    {
        TALogger.enableLog(isPrintLog);
    }
    //开发者可以根据需求设置自定义的日志系统
    public static void setLogger(ITALogger logger)
    {
        TALogger.setLogger(logger);
    }
}
