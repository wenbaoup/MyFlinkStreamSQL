package com.yjp.flink.sql.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

public class StringIsNotNullUDF extends ScalarFunction {
    private static final long serialVersionUID = -5095363473212038857L;

    public Boolean eval(String data) {
        return !StringUtils.isEmpty(data);
    }
}
