package com.yjp.flink.sql.udf;

import com.yjp.flink.sql.util.DateUtilss;
import org.apache.flink.table.functions.ScalarFunction;

public class DateToLongUDF extends ScalarFunction {
    private static final long serialVersionUID = 1791054170244089315L;

    public Long eval(String date) {
        return DateUtilss.changeStringToDate(date).getTime();
    }

    public Long eval(String date, String pattern) {
        return DateUtilss.changeStringToDate(date, pattern).getTime();
    }
}
