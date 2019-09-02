package com.yjp.flink.sql.enums;

/**
 * @Author : WenBao
 * Date : 14:21 2019/9/2
 */
public enum ECheckPointMode {
    /**
     * flink  checkpoint EXACTLY_ONCE
     */
    EXACTLY_ONCE,
    /**
     * flink  checkpoint AT_LEAST_ONCE
     */
    AT_LEAST_ONCE
}
