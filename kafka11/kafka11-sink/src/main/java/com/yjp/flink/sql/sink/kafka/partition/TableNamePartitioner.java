package com.yjp.flink.sql.sink.kafka.partition;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.types.Row;

/**
 * 自定义分区方式
 */
public class TableNamePartitioner<R> extends FlinkKafkaPartitioner<Row> {

    private static final long serialVersionUID = -6594653781664181009L;

    @Override
    public int partition(Row record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        //最后一位是tableName
        String tableName = (String) record.getField(record.getArity() - 1);
        //保证同一个tableName发送到同一个分区
        return tableName.hashCode() % partitions.length;
    }
}
