package cn.thinkingdata.analytics.inter;

import cn.thinkingdata.analytics.exception.InvalidArgumentException;

import java.util.Map;

public interface ITDAnalytics {
    /**
     * report event
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param eventName  name
     * @param properties properties
     * @throws InvalidArgumentException data error
     */
    public void track(String accountId, String distinctId, String eventName, Map<String, Object> properties)
            throws InvalidArgumentException;

    /**
     * first event
     * @param accountId     account ID
     * @param distinctId    distinct ID
     * @param eventName     name
     * @param firstCheckId  it is flag of the first event
     * @param properties    properties
     * @throws InvalidArgumentException   data error
     */
    public void trackFirst(String accountId, String distinctId, String eventName, String firstCheckId, Map<String, Object> properties)
            throws InvalidArgumentException;

    /**
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param eventName  name
     * @param eventId    event ID
     * @param properties properties
     * @throws InvalidArgumentException data error
     */
    public void trackUpdate(String accountId, String distinctId, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException;

    /**
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param eventName  event name
     * @param eventId    event ID
     * @param properties properties
     * @throws InvalidArgumentException data error
     */
    public void trackOverwrite(String accountId, String distinctId, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException;

    /**
     * set common properties. it will add to event automatically
     *
     * @param properties common properties
     */
    public void setSuperProperties(Map<String, Object> properties);

    /**
     * get super properties
     * @return Map
     */
    public Map<String, Object> getSuperProperties();

    /**
     * delete property by key
     * @param key property name
     */
    public void unsetSuperProperties(String key);

    /**
     * clear common properties
     */
    public void clearSuperProperties();

    /**
     * set common properties dynamically.
     * not recommend to add the operation which with a lot of computation
     * @param dynamicSuperPropertiesTracker properties
     */
    public void setDynamicSuperPropertiesTracker(DynamicSuperPropertiesTracker dynamicSuperPropertiesTracker);

    /**
     * delete user (not recommended)
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @throws InvalidArgumentException data error
     */
    public void userDelete(String accountId, String distinctId)
            throws InvalidArgumentException;

    /**
     * to accumulate operations against the property
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException data error
     */
    public void userAdd(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException;
    /**
     * set user properties. If such property had been set before, this message would be neglected.
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException data error
     */
    public void userSetOnce(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException;
    /**
     * set user properties. would overwrite existing names
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException data error
     */
    public void userSet(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException;
    /**
     * clear the user properties of users
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException data error
     */
    public void userUnset(String accountId, String distinctId, String... properties)
            throws InvalidArgumentException;
    /**
     * to add user properties of array type
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException data error
     */
    public void userAppend(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException;
    /**
     * append user properties to array type by unique.
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException data error
     */
    public void userUniqAppend(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException;

    /**
     * report data immediately
     */
    public void flush();

    /**
     * close and exit sdk
     */
    public void close();
}
