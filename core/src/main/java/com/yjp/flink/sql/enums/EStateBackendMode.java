package com.yjp.flink.sql.enums;

public enum EStateBackendMode {
    /**
     * RocksDB  可增量checkPoint
     */
    RocksDBStateBackend,
    /**
     * FsState
     */
    FsStateBackend
}
