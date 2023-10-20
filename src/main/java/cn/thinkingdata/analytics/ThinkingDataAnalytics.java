package cn.thinkingdata.analytics;

import cn.thinkingdata.analytics.exception.InvalidArgumentException;
import cn.thinkingdata.analytics.inter.ITDConsumer;
import java.util.*;

/**
 * @deprecated please use TDAnalytics instead
 */
@Deprecated
public class ThinkingDataAnalytics extends TDAnalytics {
    /**
     * Construct SDK
     *
     * @param consumer BatchConsumer, LoggerConsumer, DebugConsumer
     */
    public ThinkingDataAnalytics(final ITDConsumer consumer) {
        this(consumer,false);
    }

    /**
     * Construct SDK
     *
     * @param consumer BatchConsumer, LoggerConsumer, DebugConsumer
     * @param enableUUID enable uuid
     */
    public ThinkingDataAnalytics(final ITDConsumer consumer, final boolean enableUUID) {
        this(consumer,enableUUID, consumer instanceof TDDebugConsumer);
    }

    /**
     * Construct SDK
     *
     * @param consumer BatchConsumer, LoggerConsumer, DebugConsumer
     * @param enableUUID enable uuid
     * @param isStrictModel check event data
     */
    public ThinkingDataAnalytics(final ITDConsumer consumer, final boolean enableUUID, final boolean isStrictModel) {
        super(consumer, enableUUID, isStrictModel);
    }

    /**
     * Delete a user, This operation is not reversible
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @throws InvalidArgumentException exception
     */
    public void user_del(String accountId, String distinctId)
            throws InvalidArgumentException {
        userDelete(accountId, distinctId);
    }

    /**
     * To accumulate operations against the property
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    public void user_add(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        userAdd(accountId, distinctId, properties);
    }

    /**
     * Set user properties, this message would be neglected If such property had been set before
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    public void user_setOnce(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        userSetOnce(accountId, distinctId, properties);
    }

    /**
     * Set user properties. would overwrite existing names
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    public void user_set(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        userSet(accountId, distinctId, properties);
    }

    /**
     * Clear the user properties of users
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    public void user_unset(String accountId, String distinctId, String... properties)
            throws InvalidArgumentException {
        userUnset(accountId, distinctId, properties);
    }

    /**
     * To add user properties of array type
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    public void user_append(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        userAppend(accountId, distinctId, properties);
    }

    /**
     * Append user properties to array type by unique.
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException exception
     */
    public void user_uniqAppend(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException {
        userUniqAppend(accountId, distinctId, properties);
    }

    /**
     * Report first event
     * @param accountId account ID
     * @param distinctId distinct ID
     * @param eventName event name
     * @param properties properties (must add "#first_check_id" in properties, because it is flag of the first event)
     * @throws InvalidArgumentException exception
     */
    public void track_first(String accountId, String distinctId, String eventName, Map<String, Object> properties)
            throws InvalidArgumentException {
        trackFirst(accountId, distinctId, eventName, null, properties);
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
    public void track_update(String accountId, String distinctId, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException {
        trackUpdate(accountId, distinctId, eventName, eventId, properties);
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
    public void track_overwrite(String accountId, String distinctId, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException {
        trackOverwrite(accountId, distinctId, eventName, eventId, properties);
    }
}
