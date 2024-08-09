package cn.thinkingdata.analytics;

import cn.thinkingdata.analytics.exception.InvalidArgumentException;
import cn.thinkingdata.analytics.inter.DynamicSuperPropertiesTracker;
import cn.thinkingdata.analytics.inter.ITDAnalytics;
import cn.thinkingdata.analytics.inter.ITDConsumer;
import cn.thinkingdata.analytics.inter.ITDLogger;
import cn.thinkingdata.analytics.util.TDCommonUtil;
import cn.thinkingdata.analytics.util.TDLogger;
import cn.thinkingdata.analytics.util.TDPropertyUtil;
import org.apache.http.util.TextUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static cn.thinkingdata.analytics.TDConstData.LIB_NAME;
import static cn.thinkingdata.analytics.TDConstData.LIB_VERSION;

/**
 * Entry of SDK
 */
public class TDAnalytics implements ITDAnalytics {
    /**
     * consumer
     */
    private final ITDConsumer consumer;
    /**
     * common properties
     */
    private final Map<String, Object> superProperties;
    private final boolean enableUUID;
    /**
     * Incorrect data will be reported to the TE when it is false; true would be not. default false
     */
    boolean isStrictModel;
    /**
     * dynamic common properties
     */
    private DynamicSuperPropertiesTracker dynamicSuperProperties = null;
    /**
     * Construct function
     *
     * @param consumer BatchConsumer or LoggerConsumer or DebugConsumer
     */
    public TDAnalytics(final ITDConsumer consumer) {
        this(consumer,false);
    }

    /**
     * Construct function
     *
     * @param consumer BatchConsumer or LoggerConsumer or DebugConsumer
     * @param enableUUID Whether to allow uuid creation for events automatically
     */
    public TDAnalytics(final ITDConsumer consumer, final boolean enableUUID) {
        this(consumer, enableUUID, consumer instanceof TDDebugConsumer);
    }

    /**
     * Construct function
     *
     * @param consumer BatchConsumer or LoggerConsumer or DebugConsumer
     * @param enableUUID Whether to allow uuid creation for events automatically
     * @param isStrictModel Whether to enable property verification
     */
    public TDAnalytics(final ITDConsumer consumer, final boolean enableUUID, final  boolean isStrictModel) {
        this.consumer = consumer;
        this.enableUUID = enableUUID;
        this.superProperties = new ConcurrentHashMap<>();
        this.isStrictModel = isStrictModel;
    }

    /**
     * Is enabled log or not. it is enabled in DebugConsumer
     *
     * @param isPrintLog enabled
     */
    public static void enableLog(boolean isPrintLog)
    {
        TDLogger.enableLog(isPrintLog);
    }

    /**
     * Developers can set up a custom logging system based on requirements
     *
     * @param logger custom logger
     */
    public static void setLogger(ITDLogger logger)
    {
        TDLogger.setLogger(logger);
    }

    /**
     * Report normal event
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param eventName  name
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    @Override
    public void track(String accountId, String distinctId, String eventName, Map<String, Object> properties) throws InvalidArgumentException {
        innerTrack(accountId, distinctId, TDConstData.DataType.TRACK, eventName, null, properties);
    }

    /**
     * Report first event
     *
     * @param accountId account ID
     * @param distinctId distinct ID
     * @param eventName event name
     * @param properties properties (must add "#first_check_id" in properties, because it is flag of the first event)
     * @throws InvalidArgumentException exception
     */
    @Override
    public void trackFirst(String accountId, String distinctId, String eventName, String firstCheckId, Map<String, Object> properties) throws InvalidArgumentException {
        if (firstCheckId != null) {
            properties.put("#first_check_id", firstCheckId);
        }
        if (properties.containsKey("#first_check_id")) {
            innerTrack(accountId, distinctId, TDConstData.DataType.TRACK, eventName, null, properties);
        } else {
            throw new InvalidArgumentException("#first_check_id key must set");
        }
    }

    /**
     * Report update event
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param eventName  event name
     * @param eventId    event id
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    @Override
    public void trackUpdate(String accountId, String distinctId, String eventName, String eventId, Map<String, Object> properties) throws InvalidArgumentException {
        innerTrack(accountId, distinctId, TDConstData.DataType.TRACK_UPDATE, eventName, eventId, properties);
    }

    /**
     * Report overwrite event
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param eventName  event name
     * @param eventId    event id
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    @Override
    public void trackOverwrite(String accountId, String distinctId, String eventName, String eventId, Map<String, Object> properties) throws InvalidArgumentException {
        innerTrack(accountId, distinctId, TDConstData.DataType.TRACK_OVERWRITE, eventName, eventId, properties);
    }

    /**
     * Sets the properties that each event carries
     *
     * @param properties common properties
     */
    @Override
    public void setSuperProperties(Map<String, Object> properties) {
        this.superProperties.putAll(properties);
    }

    @Override
    public Map<String, Object> getSuperProperties() {
        return this.superProperties;
    }

    @Override
    public void unsetSuperProperties(String key) {
        if (!key.isEmpty()) {
            this.superProperties.remove(key);
        }
    }

    @Override
    public void clearSuperProperties() {
        this.superProperties.clear();
    }

    @Override
    public void setDynamicSuperPropertiesTracker(DynamicSuperPropertiesTracker dynamicSuperPropertiesTracker) {
        this.dynamicSuperProperties = dynamicSuperPropertiesTracker;
    }

    /**
     * Delete a user, This operation is not reversible
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @throws InvalidArgumentException exception
     */
    @Override
    public void userDelete(String accountId, String distinctId) throws InvalidArgumentException {
        innerUserProfile(accountId, distinctId, TDConstData.DataType.USER_DEL, null);
    }

    /**
     * To accumulate operations against the property
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    @Override
    public void userAdd(String accountId, String distinctId, Map<String, Object> properties) throws InvalidArgumentException {
        innerUserProfile(accountId, distinctId, TDConstData.DataType.USER_ADD, properties);
    }

    /**
     * Set user properties, this message would be neglected If such property had been set before
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    @Override
    public void userSetOnce(String accountId, String distinctId, Map<String, Object> properties) throws InvalidArgumentException {
        innerUserProfile(accountId, distinctId, TDConstData.DataType.USER_SET_ONCE, properties);
    }

    /**
     * Set user properties. would overwrite existing names
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    @Override
    public void userSet(String accountId, String distinctId, Map<String, Object> properties) throws InvalidArgumentException {
        innerUserProfile(accountId, distinctId, TDConstData.DataType.USER_SET, properties);
    }

    /**
     * Clear the user properties of users
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    @Override
    public void userUnset(String accountId, String distinctId, String... properties) throws InvalidArgumentException {
        if (properties == null) {
            return;
        }
        Map<String, Object> prop = new HashMap<>();
        for (String s : properties) {
            prop.put(s, 0);
        }
        innerUserProfile(accountId, distinctId, TDConstData.DataType.USER_UNSET, prop);
    }

    /**
     * To add user properties of array type
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    @Override
    public void userAppend(String accountId, String distinctId, Map<String, Object> properties) throws InvalidArgumentException {
        innerUserProfile(accountId, distinctId, TDConstData.DataType.USER_APPEND, properties);
    }

    /**
     * Append user properties to array type by unique.
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    @Override
    public void userUniqAppend(String accountId, String distinctId, Map<String, Object> properties) throws InvalidArgumentException {
        innerUserProfile(accountId, distinctId, TDConstData.DataType.USER_UNIQ_APPEND, properties);
    }

    @Override
    public void flush() {
        this.consumer.flush();
        TDLogger.println("Manually flush");
    }

    @Override
    public void close() {
        this.consumer.close();
        TDLogger.println("SDK close");
    }

    private void innerTrack(String accountId, String distinctId, TDConstData.DataType type, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, type, eventName, eventId, properties);
    }

    private void innerUserProfile(String accountId, String distinctId, TDConstData.DataType type, Map<String, Object> properties)
            throws InvalidArgumentException {
        add(distinctId, accountId, type, null, null, properties);
    }

    private void add(String distinctId, String accountId, TDConstData.DataType type, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException {
        if (isStrictModel && TextUtils.isEmpty(accountId) && TextUtils.isEmpty(distinctId)) {
            throw new InvalidArgumentException("accountId or distinctId must be provided.");
        }
        Map<String, Object> copyProperties =  properties!=null ? new ConcurrentHashMap<>(properties) : new ConcurrentHashMap<String, Object>();
        Map<String, Object> desProperties =  new HashMap<>();
        Map<String, Object> event = new HashMap<>();
        TDCommonUtil.buildData(event,"#distinct_id", distinctId);
        TDCommonUtil.buildData(event,"#account_id", accountId);
        event.put("#time", new Date());
        event.put("#type", type.getType());
        // #uuid v4 is only supported
        if(!event.containsKey("#uuid") && enableUUID)
        {
            event.put("#uuid", UUID.randomUUID().toString());
        }
        // Move special properties
        TDPropertyUtil.moveProperty(event,copyProperties,"#uuid","#time","#ip","#app_id","#first_check_id","#transaction_property","#import_tool_id");
        if (type.getType().contains("track")) {
            if (isStrictModel) {
                TDCommonUtil.throwEmptyException(eventName,"The event name must be provided.");
            }
            if (type.getType().contains("track_")) {
                if (isStrictModel) {
                    TDCommonUtil.throwEmptyException(eventId,"The event id must be provided.");
                }
                TDCommonUtil.buildData(event,"#event_id", eventId);
            }
            TDPropertyUtil.mergeProperties(desProperties,superProperties,dynamicSuperProperties!=null ? dynamicSuperProperties.getDynamicSuperProperties():null,copyProperties);
            event.put("#event_name", eventName);
            desProperties.put("#lib", LIB_NAME);
            desProperties.put("#lib_version", LIB_VERSION);
        } else {
            TDPropertyUtil.mergeProperties(desProperties,copyProperties);
        }
        if (isStrictModel) {
            TDPropertyUtil.assertProperties(desProperties,type);
        }
        event.put("properties",desProperties);
        try {
            this.consumer.add(event);
        } catch (Exception e) {
            throw new InvalidArgumentException(e);
        }
    }
}
