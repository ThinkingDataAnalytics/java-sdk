package cn.thinkingdata.analytics.util;

import cn.thinkingdata.analytics.inter.ITDLogger;
import cn.thinkingdata.analytics.TDConstData;

public class TDLogger {
    private static ITDLogger loggerInstance;
    private static boolean isPrintLog = false;

    public static void enableLog(boolean isPrint) {
        isPrintLog = isPrint;
    }

    public static void setLogger(ITDLogger logger) {
        loggerInstance = logger;
    }

    public static void print(String msg) {
        if (isPrintLog) {
            String formatMsg = String.format("[ThinkingData] %s", msg);
            if (loggerInstance != null) {
                loggerInstance.print(formatMsg);
            } else {
                System.out.print(formatMsg);
            }
        }
    }

    public static void println(String msg) {
        if (isPrintLog) {
            String formatMsg = String.format("%s\n", msg);
            TDLogger.print(formatMsg);
        }
    }
}
