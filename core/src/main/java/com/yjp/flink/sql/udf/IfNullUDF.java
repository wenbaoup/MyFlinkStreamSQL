package com.yjp.flink.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class IfNullUDF extends ScalarFunction {
    private static final long serialVersionUID = -5095363473212038857L;

    public String eval(String data, String defaultValue) {
        if (null == data) {
            return defaultValue;
        }
        return data;
    }
}
