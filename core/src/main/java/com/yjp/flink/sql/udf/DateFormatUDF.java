package com.yjp.flink.sql.udf;

import com.yjp.flink.sql.util.DateUtilss;
import org.apache.flink.table.functions.ScalarFunction;


public class DateFormatUDF extends ScalarFunction {
    private static final long serialVersionUID = -4281108994653417053L;

    public String eval(String date, String pattern) {
        return DateUtilss.transDateToString(DateUtilss.changeStringToDate(date), pattern);
    }

}
