package com.yjp.flink.sql.udf;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

public class TimestampToLongUdf extends ScalarFunction {
    @Override
    public void open(FunctionContext context) {
    }

    public static Long eval(String timestamp) {
        if (timestamp.length() == 13) {
            return Long.parseLong(timestamp);
        } else if (timestamp.length() == 10) {
            return Long.parseLong(timestamp) * 1000;
        } else {
            return Timestamp.valueOf(timestamp).getTime();
        }
    }

    @Override
    public void close() {
    }
}
