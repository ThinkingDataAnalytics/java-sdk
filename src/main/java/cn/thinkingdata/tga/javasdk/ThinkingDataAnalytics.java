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
    private final Map<String, Object> superProperties; // common properties
    private final boolean enableUUID;
    // Incorrect data will be reported to the TE when it is false; true would be not. default false, 
    boolean isStrictModel = false;    
    // dynamic common properties              
    private DynamicSuperPropertiesTracker dynamicSuperProperties = null;
    /**
     * construct function
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
     * delete a user, This operation is not reversible
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @throws InvalidArgumentException exception
     */
    public void user_del(String accountId, String distinctId)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.USER_DEL, null);
    }
    /**
     * to accumulate operations against the property
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    public void user_add(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.USER_ADD, properties);
    }

    /**
     * set user properties, this message would be neglected If such property had been set before
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    public void user_setOnce(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.USER_SET_ONCE, properties);
    }

    /**
     * set user properties. would overwrite existing names
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    public void user_set(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.USER_SET, properties);
    }

    /**
     * clear the user properties of users
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
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
     * to add user properties of array type
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    public void user_append(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.USER_APPEND, properties);
    }

    /**
     * append user properties to array type by unique.
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    public void user_uniqAppend(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.USER_UNIQ_APPEND, properties);
    }

    /**
     * ordinary event report
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param eventName  event name
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    public void track(String accountId, String distinctId, String eventName, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.TRACK, eventName, null, properties);
    }
    /**
     * report first event
     * @param accountId     account ID
     * @param distinctId    distinct ID
     * @param eventName     event name
     * @param properties    properties
     *                      (must add "#first_check_id" in properties, because it is flag of the first event)
     * @throws InvalidArgumentException   exception
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
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param eventName  event name
     * @param eventId    event id
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    public void track_update(String accountId, String distinctId, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.TRACK_UPDATE, eventName, eventId,properties);
    }

    /**
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param eventName  event name
     * @param eventId    event id
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    public void track_overwrite(String accountId, String distinctId, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, DataType.TRACK_OVERWRITE, eventName, eventId,properties);
    }

    /**
     * clear common properties
     */
    public void clearSuperProperties() {
        this.superProperties.clear();
    }

    /**
     * set common properties
     *
     * @param properties properties
     */
    public void setSuperProperties(Map<String, Object> properties) {
        this.superProperties.putAll(properties);
    }

    /**
     * set common properties dynamically.
     * not recommend to add the operation which with a lot of computation
     */
    public void setDynamicSuperPropertiesTracker(DynamicSuperPropertiesTracker dynamicSuperPropertiesTracker) {
        dynamicSuperProperties = dynamicSuperPropertiesTracker;
    }

    /**
     * report data immediately
     */
    public void flush() {
        this.consumer.flush();
    }

    /**
     * close and exit sdk
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
        // #uuid v4 is only supported
        if(!event.containsKey("#uuid") && enableUUID)
        {
            event.put("#uuid", UUID.randomUUID().toString());
        }
        // Move special properties
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
    // is enabled log or not. it is enabled in DebugConsumer
    public static  void enableLog(boolean isPrintLog)
    {
        TALogger.enableLog(isPrintLog);
    }

    // Developers can set up a custom logging system based on requirements
    public static void setLogger(ITALogger logger)
    {
        TALogger.setLogger(logger);
    }
}
