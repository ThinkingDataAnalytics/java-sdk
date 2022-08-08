package cn.thinkingdata.tga.javasdk.inter;
import cn.thinkingdata.tga.javasdk.exception.InvalidArgumentException;
import java.util.Map;
public interface IThinkingDataAnalytics {
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
            throws InvalidArgumentException;

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
            throws InvalidArgumentException;

    /**
     * @param accountId  账号 ID
     * @param distinctId 访客ID
     * @param eventName  事件名称
     * @param eventId    事件ID
     * @param properties 事件属性
     * @throws InvalidArgumentException 数据错误
     */
    public void track_update(String accountId, String distinctId, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException ;
    /**
     * @param accountId  账号 ID
     * @param distinctId 访客ID
     * @param eventName  事件ID
     * @param eventId    事件ID
     * @param properties 事件属性
     * @throws InvalidArgumentException 数据错误
     */
    public void track_overwrite(String accountId, String distinctId, String eventName, String eventId, Map<String, Object> properties)
            throws InvalidArgumentException;

    /**
     * 清除公共事件属性
     */
    public void clearSuperProperties();

    /**
     * 设置公共事件属性. 公共事件属性会添加到每个事件的属性中上报
     *
     * @param properties 公共属性
     */
    public void setSuperProperties(Map<String, Object> properties);

    /**
     * 设置动态公共属性，即此处设置的公共属性会在上报时获取值
     * 建议此回调方法中不要加入大量计算操作代码
     * @param dynamicSuperPropertiesTracker 动态公共属性
     */
    public void setDynamicSuperPropertiesTracker(DynamicSuperPropertiesTracker dynamicSuperPropertiesTracker);


    /**
     * 删除用户，此操作不可逆
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @throws InvalidArgumentException 数据错误
     */
    public void user_del(String accountId, String distinctId)
            throws InvalidArgumentException ;

    /**
     * 用户属性修改，只支持数字属性增加的接口
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_add(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException;
    /**
     * 设置用户属性. 如果该属性已经存在，该操作无效.
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_setOnce(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException ;
    /**
     * 设置用户属性. 如果属性已经存在，则覆盖; 否则，新创建用户属性
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_set(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException;
    /**
     * 删除用户指定的属性
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_unset(String accountId, String distinctId, String... properties)
            throws InvalidArgumentException;
    /**
     * 用户的数组类型的属性追加
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_append(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException ;
    /**
     * 用户的数组类型的属性去重追加
     *
     * @param accountId  账号 ID
     * @param distinctId 访客 ID
     * @param properties 用户属性
     * @throws InvalidArgumentException 数据错误
     */
    public void user_uniqAppend(String accountId, String distinctId, Map<String, Object> properties)
            throws InvalidArgumentException ;

    /**
     * 立即上报数据到接收端
     */
    public void flush();
    /**
     * 关闭并退出 sdk 所有线程，停止前会清空所有本地数据
     */
    public void close();



}
