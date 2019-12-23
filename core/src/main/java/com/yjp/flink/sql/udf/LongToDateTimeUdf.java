package com.yjp.flink.sql.udf;

import com.yjp.flink.sql.util.DateUtilss;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public class LongToDateTimeUdf extends ScalarFunction {
    private static final long serialVersionUID = 9219105775556150210L;

    @Override
    public void open(FunctionContext context) {
    }

    public static String eval(Long timestamp, String pattern) {
        return DateUtilss.parseTimeStampToDateString(timestamp, pattern);
    }

    @Override
    public void close() {
    }
}
