package cn.thinkingdata.tga.javasdk.util;

import cn.thinkingdata.tga.javasdk.TAConstData;
import cn.thinkingdata.tga.javasdk.inter.ITALogger;

public class TALogger {
    private static ITALogger loggerInstance;
    private static boolean isPrintLog = false;

    public static void enableLog(boolean isPrint) {
        isPrintLog = isPrint;
    }

    public static void setLogger(ITALogger logger) {
        loggerInstance = logger;
    }

    public static void print(String msg) {
        if (isPrintLog) {
            String formatMsg = String.format("[ThinkingAnalytics-Java SDK V%s]-%s", TAConstData.LIB_VERSION, msg);
            if (loggerInstance != null) {
                loggerInstance.print(formatMsg);
            } else {
                System.out.println(formatMsg);
            }
        }
    }
}
