package cn.thinkingdata.tga.javasdk;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ExampleSDK {

    public static void main(String[] args) throws Exception {
        //LoggerConsumer 有三种实例方式，根据业务需求设置，选择一种即可
        /**
         * 1.该方式在1.3.1版本后不默认以大小切分文件，只按天切分文件
         */
        String log_dirctory = "/home/logdata";  //设定logbus监控的目录
        ThinkingDataAnalytics tga = new ThinkingDataAnalytics(new ThinkingDataAnalytics.LoggerConsumer(log_dirctory));
        /**
         * 2.该方式可以设置文件大小切分，以天切分为前提,这里设置的是5GB
         */
        //ThinkingDataAnalytics tga = new ThinkingDataAnalytics(new ThinkingDataAnalytics.LoggerConsumer(log_dirctory,5*1024));
        /**
         * 3.该方式是在版本1.2.0以后的实例，之前的实例方式也保留着，版本1.3.1以后默认大小1GB 取消，用户可根据数据量设计按小时切分还是按大小切分，默认按天切分
         */
        //ThinkingDataAnalytics.LoggerConsumer.Config config = new ThinkingDataAnalytics.LoggerConsumer.Config(log_dirctory);
        //config.setRotateMode(ThinkingDataAnalytics.LoggerConsumer.RotateMode.DAILY);//可以设置是按天(DAILY)切分，还是按小时（HOURLY）切分，默认按天切分文件，可不设置
        //config.setFileSize(2*1024);//设置在按天切分的前提下，按大小切分文件，可不设置
        //config.setBufferSize(8192);//默认是8192字节(8k)然后刷新数据(flush)，可设置字节
        //ThinkingDataAnalytics tga = new ThinkingDataAnalytics(new ThinkingDataAnalytics.LoggerConsumer(config));


        //BatchConsumer
        //ThinkingDataAnalytics tga = new ThinkingDataAnalytics(new ThinkingDataAnalytics.BatchConsumer("url", "appid"));

        //DebugConsumer一条一条的发送，用于测试数据格式是否正确
        //ThinkingDataAnalytics tga = new ThinkingDataAnalytics(new ThinkingDataAnalytics.DebugConsumer("url", "appid"));
        // 1. 用户匿名访问网站
        String distinct_id = "SDIF21dEJWsI232IdSJ232d2332"; // 用户未登录时，可以使用产品自己生成的cookieId等唯一标识符来标注用户
        Map<String, Object> properties = new HashMap<String, Object>();
        // 1.1 访问首页

        // 前面有#开头的property字段，是tga提供给用户的预置字段
        // 对于预置字段，已经确定好了字段类型和字段的显示名
        properties.clear();
        properties.put("#time", new Date());                // 这条event发生的时间，如果不设置的话，则默认是当前时间***#time的时间类型必须是Date***
        properties.put("#os", "Windows");                   // 用户使用设备的操作系统
        properties.put("#os_version", "8.1");               // 操作系统的具体版本
        properties.put("#ip", "123.123.123.123");           // 请求中能够拿到用户的IP，则把这个传递给tga，tga会自动根据这个解析省份、城市
        properties.put("Channel", "百度");                 // 用户是通过baidu这个渠道过来的
        tga.track(distinct_id, null,"ViewHomePage", properties); // 记录访问首页这个event

        //2.用户注册
        properties.clear();
        properties.put("#time", new Date());
        tga.track("distinct_id", "account_id", "signup", properties);
        //3.注册用户的基本资料
        properties.clear();
        properties.put("#time", new Date());
        properties.put("#ip", "123.123.123.123");
        properties.put("name","username");
        properties.put("age", 18);
        properties.put("level", 10);
        tga.user_setOnce("account_id",null, properties);

        //4.年龄改为20
        properties.clear();
        properties.put("age", 20);
        properties.put("#time", new Date());
        tga.user_set("account_id",null, properties);

        //5.level增加了3级(降了用-3)
        properties.clear();
        properties.put("level", 3);
        properties.put("#time", new Date());
        properties.put("#testkey",6666);
        tga.user_add("account_id",null, properties);

        //*****************************
        //7.公共属性
        HashMap<String, Object> super_properties = new HashMap<String,Object>();
        super_properties.put("#country", "中国");
        super_properties.put("source", "media");
        properties.put("#ip", "123.123.123.123");
        tga.setSuperProperties(super_properties);
        //6.注册用户购买了商品a和b
        properties.clear();
        properties.put("#time", new Date());
        properties.put("Product_Name", "a");
        properties.put("Price", 9.9);
        properties.put("OrderId", "order_id_a");
        tga.track("account_id",null, "Product_Purchase", properties);

        properties.clear();
        properties.put("#time", new Date());
        properties.put("Product_Name", "b");
        properties.put("Price", 13.2);
        properties.put("OrderId", "order_id_b");
        tga.track("account_id",null, "Product_Purchase", properties);

        //8.撤销对商品b的购买
        properties.clear();
        properties.put("#os", "Windows");         //
        properties.put("OrderId", "order_id_b");   // 订单ID
        properties.put("ShipPrice", 10.0);             // 运费
        properties.put("OrderTotalPrice", 13.2);         // 订单的商品价格
        properties.put("CancelReason", "不想买了"); // 取消订单的原因
        properties.put("CancelTiming", "AfterPay");   // 取消订单的时机
        properties.put("#time", new Date());
        tga.track("account_id",null, "CancelOrder", properties);

        //9.清除公共属性
        tga.clearSuperProperties();

        //10.用户有浏览了e商品
        properties.clear();
        properties.put("#time", new Date());
        properties.put("#ip", "123.123.123.123");
        properties.put("Product_Name", "e");
        tga.track("account_id",null, "Browse_Product", properties);

        tga.close();

    }

}
