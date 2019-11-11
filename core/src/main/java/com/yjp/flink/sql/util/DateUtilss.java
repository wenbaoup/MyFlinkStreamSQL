package com.yjp.flink.sql.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DateUtilss {
    public static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
    public static final String YYYY_MM_DD_HH_MM_SS_SSS = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String YYYY_MM_DD_HH = "yyyyMMddHH";
    public static final String YYYY_MM_DD_HH_MM = "yyyyMMddHHmm";
    public static final String YYYY_MM_DD = "yyyyMMdd";
    private static final Object lockObj = new Object();
    private static Map<String, ThreadLocal<SimpleDateFormat>> sdfMap = new HashMap();

    public DateUtilss() {
    }

    private static SimpleDateFormat getSdf(String pattern) {
        ThreadLocal<SimpleDateFormat> tl = (ThreadLocal) sdfMap.get(pattern);
        if (tl == null) {
            Object var2 = lockObj;
            synchronized (lockObj) {
                tl = (ThreadLocal) sdfMap.get(pattern);
                if (tl == null) {
                    tl = ThreadLocal.withInitial(() -> {
                        return new SimpleDateFormat(pattern);
                    });
                    sdfMap.put(pattern, tl);
                }
            }
        }

        return (SimpleDateFormat) tl.get();
    }

    public static String transDateToString(Date date, String pattern) {
        return null == date ? " " : getSdf(pattern).format(date);
    }

    public static String transSqlDateToString(java.sql.Date date, String pattern) {
        return transDateToString(new Date(date.getTime()), pattern);
    }

    public static Date tranSqlDateToUtilDate(java.sql.Date date) {
        return new Date(date.getTime());
    }

    public static Date parseDateStringToDate(String dateStr, String pattern) {
        try {
            return getSdf(pattern).parse(dateStr);
        } catch (ParseException var3) {
            throw new RuntimeException(var3);
        }
    }

    public static String parseTimeStampToDateString(Long ts, String pattern) {
        return getSdf(pattern).format(ts);
    }

    public static Date parseTimeStampToDate(Long ts, String pattern) {
        return parseDateStringToDate(parseTimeStampToDateString(ts, pattern), pattern);
    }

    public static Date changeStringToDate(String dateStr) {
        return parseDateStringToDate(dateStr, "yyyy-MM-dd HH:mm:ss");
    }

    public static boolean compareDate(Date newDate, Date oldDate) {
        if (newDate == null && oldDate == null) {
            return false;
        } else if (newDate != null) {
            return oldDate == null || newDate.after(oldDate);
        } else {
            return false;
        }
    }
}
