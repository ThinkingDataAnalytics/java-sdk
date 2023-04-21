package cn.thinkingdata.tga.javasdk;

import java.util.regex.Pattern;

public class TAConstData {
    public final static String LIB_VERSION = "2.1.2-beta.2";
    public final static String LIB_NAME = "tga_java_sdk";
    public final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    // A string of 50 letters and digits that starts with '#' or a letter
    public final static Pattern KEY_PATTERN = Pattern.compile(
            "^(#[a-z][a-z\\d_]{0,49})|([a-z][a-z\\d_]{0,50})|(__[a-z][a-z\\d_]{0,50})$", Pattern.CASE_INSENSITIVE);

    public enum DataType {
        /**
         * SDK api
         */
        TRACK("track"),
        USER_SET("user_set"),
        USER_SET_ONCE("user_setOnce"),
        USER_ADD("user_add"),
        USER_DEL("user_del"),
        USER_UNSET("user_unset"),
        USER_APPEND("user_append"),
        USER_UNIQ_APPEND("user_uniq_append"),
        TRACK_UPDATE("track_update"),
        TRACK_OVERWRITE("track_overwrite");

        private final String type;

        DataType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }
}
