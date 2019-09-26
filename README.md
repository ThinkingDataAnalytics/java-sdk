# ThinkingData Analytics Java SDK

详细的使用文档请参考 [Java SDK API文档](https://doc.thinkingdata.cn/tdamanual/installation/java_sdk_installation.html)

### 1. 初始化 SDK

1. 使用 Maven 集成 SDK，请在`pom.xml`文件中置入以下依赖信息：

```xml
<dependencies>
    // others...
    <dependency>
        <groupId>cn.thinkingdata</groupId>
        <artifactId>thinkingdatasdk</artifactId>
        <version>1.2.0</version>
    </dependency>
</dependencies>
```

2. 初始化 SDK

您可以通过 3 种方式获得 SDK 实例:

**(1) LoggerConsumer:** 批量实时写本地文件，文件以天/小时为分隔，需要搭配 LogBus 进行上传

```java
// 使用LoggerConsumer
ThinkingDataAnalytics ta = new ThinkingDataAnalytics(new ThinkingDataAnalytics.LoggerConsumer(LOG_DIRECTORY));
```

`LOG_DIRECTORY` 为写入本地的文件夹地址，您只需将 LogBus 的监听文件夹地址设置为此处的地址，即可使用 LogBus 进行数据的监听上传。


**(2) BatchConsumer:** 批量实时地向TA服务器传输数据，不需要搭配传输工具，**<font color="red">不建议在正式环境中使用</font>**

```java
// 使用BatchConsumer
ThinkingDataAnalytics ta = new ThinkingDataAnalytics(new ThinkingDataAnalytics.BatchConsumer(SERVER_URL, APP_ID));
```

`SERVER_URL` 为传输数据的 url，`APP_ID` 为您的项目的 APP ID

如果您使用的是云服务，请输入以下 URL:

http://receiver.ta.thinkingdata.cn/logagent

如果您使用的是私有化部署的版本，请输入以下 URL:

http://<font color="red">数据采集地址</font>/logagent

**(3) DebugConsumer:** 逐条向 TA 服务器传输数据，在数据校验出错时会抛出异常. **<font color="red">不要在正式环境中使用</font>**
```java
ThinkingDataAnalytics ta = new ThinkingDataAnalytics(new ThinkingDataAnalytics.DebugConsumer(SERVER_URL, APP_ID));
```

DebugConsumer 的初始化参数与 BatchConsumer 一致.


### 2. 使用示例

##### a\)发送事件

您可以调用 `track` 来上传事件，建议您根据先前梳理的文档来设置事件的属性以及发送信息的条件，此处以用户付费作为范例：

```java
// 初始化SDK
ThinkingDataAnalytics ta = new ThinkingDataAnalytics(new ThinkingDataAnalytics.BatchConsumer(SERVER_URI, APP_ID));

// 设置访客ID"ABCDEFG123456789"
String distinct_id = "ABCDEFG123456789";

// 设置账号ID"TA_10001"
String account_id = "TA_10001";

// 设置事件属性
Map<String,Object> properties = new HashMap<String,Object>();

// 设置事件发生的时间，如果不设置的话，则默认使用为当前时间，**注意** #time 的类型必须是 Date 
properties.put("#time", new Date());

// 设置用户的 IP 地址，TA系统会根据IP地址解析用户的地理位置信息，如果不设置的话，则默认不上报
properties.put("#ip", "192.168.1.1");

properties.put("Product_Name", "商品A");
properties.put("Price", 30);
properties.put("OrderId", "订单号abc_123");

// 上传事件，包含用户的访客ID与账号ID，请注意不要将访客ID与账号ID写反
ta.track(account_id,distinct_id,"payment",properties);

```
* 事件的名称是 `String` 类型，只能以字母开头，可包含数字，字母和下划线 “\_”，长度最大为 50 个字符，对字母大小写不敏感。
* 事件的属性是一个 `Map<String,Object>` 对象，其中每个元素代表一个属性。  
* Key 的值为属性的名称，为`String`类型，规定只能以字母开头，包含数字，字母和下划线 “\_”，长度最大为 50 个字符，对字母大小写不敏感。  
* Value 为该属性的值，支持 `String`、`Number`、`Boolean` 和 `Date`。

##### b) 立即提交数据

```java
ta.flush();
```

立即提交数据到相应的接收器。

##### c) 关闭sdk
	
```java
ta.close();
```
关闭并退出sdk，请在关闭服务器前调用本接口，以避免缓存内的数据丢失。
