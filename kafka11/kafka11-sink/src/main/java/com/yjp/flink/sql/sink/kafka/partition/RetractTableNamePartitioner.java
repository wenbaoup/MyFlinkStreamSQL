package com.yjp.flink.sql.sink.kafka.partition;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.types.Row;

/**
 * 自定义分区方式
 */
public class RetractTableNamePartitioner<R> extends FlinkKafkaPartitioner<Row> {

    private static final long serialVersionUID = -6594653781664181009L;

    @Override
    public int partition(Row record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        //倒数第二位是tableName
        String tableName = (String) record.getField(record.getArity() - 2);
        //保证同一个tableName发送到同一个分区
        return Math.abs(tableName.hashCode() % partitions.length);
    }
}
