package cn.thinkingdata.analytics.test.functionTest;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.annotation.JSONField;

import java.util.Date;
import java.util.Map;

public class TemporaryEvent {
    @JSONField(name = "#app_id")
    protected String appId = null;
    @JSONField(name = "#account_id")
    protected String accountId = null;
    @JSONField(name = "#distinct_id")
    protected String distinctId = null;
    @JSONField(name = "#uuid")
    protected String uuid = null;
    @JSONField(name = "#type")
    protected String type;
    @JSONField(name = "#time", format = "yyyy-MM-dd HH:mm:ss.SSS")
    protected Date time;
    @JSONField(name = "#ip")
    private String ip = null;
    @JSONField(name = "#event_name")
    private String eventName = null;
    @JSONField(name = "#event_id")
    private String eventId = null;
    @JSONField(name = "#first_check_id")
    private String firstCheckId = null;
    @JSONField(name = "#transaction_property")
    private String transactionProperty = null;
    @JSONField(name = "#import_tool_id")
    private String importToolId;

    @JSONField(name = "properties")
    protected JSONObject propertyObj = null;

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getDistinctId() {
        return distinctId;
    }

    public void setDistinctId(String distinctId) {
        this.distinctId = distinctId;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getFirstCheckId() {
        return firstCheckId;
    }

    public void setFirstCheckId(String firstCheckId) {
        this.firstCheckId = firstCheckId;
    }

    public String getTransactionProperty() {
        return transactionProperty;
    }

    public void setTransactionProperty(String transactionProperty) {
        this.transactionProperty = transactionProperty;
    }

    public String getImportToolId() {
        return importToolId;
    }

    public void setImportToolId(String importToolId) {
        this.importToolId = importToolId;
    }

    public JSONObject getPropertyObj() {
        return propertyObj;
    }

    public void setPropertyObj(JSONObject propertyObj) {
        this.propertyObj = propertyObj;
    }

    public TemporaryEvent(Map<String,Object> mapData)
    {

    }
    public TemporaryEvent(JSONObject data)
    {
        this.appId = data.getString("#app_id");
        this.accountId = data.getString("#account_id");
        this.distinctId = data.getString("#distinct_id");
        this.firstCheckId = data.getString("#first_check_id");
        this.type = data.getString("#type");
        this.propertyObj = data.getJSONObject("properties");
        this.uuid = data.getString("#uuid");
        this.time = data.getDate("#time");
        this.ip = data.getString("#ip");
        this.eventName = data.getString("#event_name");
        this.importToolId = data.getString("#import_tool_id");
        this.eventId = data.getString("#event_id");
        this.transactionProperty = data.getString("#transaction_property");
    }

}
