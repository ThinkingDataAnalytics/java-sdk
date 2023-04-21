package cn.thinkingdata.tga.javasdk.inter;
import cn.thinkingdata.tga.javasdk.exception.InvalidArgumentException;
import java.util.Map;
public interface IThinkingDataAnalytics {
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
     * @param properties    properties
     *                      (must add "#first_check_id" in properties, because it is flag of the first event)
     * @throws InvalidArgumentException   data error
     */
    public void track_first(String accountId, String distinctId, String eventName, Map<String, Object> properties)
            throws InvalidArgumentException;

    /**
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param eventName  name
     * @param eventId    event ID
     * @param properties properties
     * @throws InvalidArgumentException data error
     */
    public void track_update(String accountId, String distinctId, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException;

    /**
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param eventName  event name
     * @param eventId    event ID
     * @param properties properties
     * @throws InvalidArgumentException data error
     */
    public void track_overwrite(String accountId, String distinctId, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException;

    /**
     * clear common properties
     */
    public void clearSuperProperties();

    /**
     * set common properties. it will add to event automatically
     *
     * @param properties common properties
     */
    public void setSuperProperties(Map<String, Object> properties);

    /**
     * set common properties dynamically.
     * not recommend to add the operation which with a lot of computation
     * @param dynamicSuperPropertiesTracker properties
     */
    public void setDynamicSuperPropertiesTracker(DynamicSuperPropertiesTracker dynamicSuperPropertiesTracker);


    /**
     * delete user
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @throws InvalidArgumentException data error
     */
    public void user_del(String accountId, String distinctId)
            throws InvalidArgumentException ;

    /**
     * to accumulate operations against the property
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException data error
     */
    public void user_add(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException;
    /**
     * set user properties. If such property had been set before, this message would be neglected.
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException data error
     */
    public void user_setOnce(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException ;
    /**
     * set user properties. would overwrite existing names
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException data error
     */
    public void user_set(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException;
    /**
     * clear the user properties of users
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException data error
     */
    public void user_unset(String accountId, String distinctId, String... properties)
            throws InvalidArgumentException;
    /**
     * to add user properties of array type
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException data error
     */
    public void user_append(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException ;
    /**
     * append user properties to array type by unique.
     *
     * @param accountId  account ID
     * @param distinctId distinct ID
     * @param properties properties
     * @throws InvalidArgumentException data error
     */
    public void user_uniqAppend(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException ;

    /**
     * report data immediately
     */
    public void flush();

    /**
     * close and exit sdk
     */
    public void close();

}
