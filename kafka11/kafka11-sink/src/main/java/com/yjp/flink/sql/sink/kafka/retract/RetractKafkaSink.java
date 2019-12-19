/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yjp.flink.sql.sink.kafka.retract;

import com.yjp.flink.sql.sink.IStreamSinkGener;
import com.yjp.flink.sql.sink.kafka.CustomerJsonRowSerializationSchema;
import com.yjp.flink.sql.sink.kafka.partition.RetractTableNamePartitioner;
import com.yjp.flink.sql.sink.kafka.table.KafkaSinkTableInfo;
import com.yjp.flink.sql.table.TargetTableInfo;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSinkBase;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Optional;
import java.util.Properties;

/**
 * kafka result table
 * Date: 2018/12/18
 * Company: www.dtstack.com
 *
 * @author DocLi
 * @modifyer maqi
 */
public class RetractKafkaSink implements RetractStreamTableSink<Row>, IStreamSinkGener<RetractKafkaSink> {

    protected String[] fieldNames;

    protected TypeInformation<?>[] fieldTypes;

    protected String topic;

    protected int parallelism;

    protected Properties properties;

    /**
     * Serialization schema for encoding records to Kafka.
     */
    protected SerializationSchema serializationSchema;

    /**
     * The schema of the table.
     */
    private TableSchema schema;

    /**
     * Partitioner to select Kafka partition for each item.
     */
    protected Optional<FlinkKafkaPartitioner<Row>> partitioner;


    @Override
    public RetractKafkaSink genStreamSink(TargetTableInfo targetTableInfo) {
        KafkaSinkTableInfo kafka11SinkTableInfo = (KafkaSinkTableInfo) targetTableInfo;
        this.topic = kafka11SinkTableInfo.getTopic();

        properties = new Properties();
        properties.setProperty("bootstrap.servers", kafka11SinkTableInfo.getBootstrapServers());

        for (String key : kafka11SinkTableInfo.getKafkaParamKeys()) {
            properties.setProperty(key, kafka11SinkTableInfo.getKafkaParam(key));
        }
        this.partitioner = Optional.of(new RetractTableNamePartitioner<Row>());
        this.fieldNames = kafka11SinkTableInfo.getFields();
        TypeInformation[] types = new TypeInformation[kafka11SinkTableInfo.getFields().length + 1];
        for (int i = 0; i < kafka11SinkTableInfo.getFieldClasses().length; i++) {
            types[i] = TypeInformation.of(kafka11SinkTableInfo.getFieldClasses()[i]);
        }
        types[kafka11SinkTableInfo.getFields().length] = TypeInformation.of(Integer.TYPE);
        this.fieldTypes = types;
        String[] newFieldNames = new String[fieldNames.length + 1];
        System.arraycopy(fieldNames, 0, newFieldNames, 0, fieldNames.length);
        newFieldNames[fieldNames.length] = "retract";
        this.fieldNames = newFieldNames;
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (int i = 0; i < fieldNames.length; i++) {
            schemaBuilder.field(fieldNames[i], fieldTypes[i]);
        }
        this.schema = schemaBuilder.build();

        Integer parallelism = kafka11SinkTableInfo.getParallelism();
        if (parallelism != null) {
            this.parallelism = parallelism;
        }
        this.serializationSchema = new CustomerJsonRowSerializationSchema(getOutputType().getTypeAt(1));
        System.out.println(Thread.currentThread().getName());
        return this;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        System.out.println(Thread.currentThread().getName());
        KafkaTableSinkBase kafkaTableSink = new RetractCustomerKafka11JsonTableSink(
                schema,
                topic,
                properties,
                partitioner,
                serializationSchema
        );
        DataStream<Row> ds = dataStream.flatMap(new FlatMapFunction<Tuple2<Boolean, Row>, Row>() {
            private static final long serialVersionUID = 3930272552743751908L;

            @Override
            public void flatMap(Tuple2<Boolean, Row> value, Collector<Row> out) throws Exception {
                Row oldRow = value.f1;
                Row newRow = new Row(oldRow.getArity() + 1);
                for (int i = 0; i < oldRow.getArity(); i++) {
                    newRow.setField(i, oldRow.getField(i));
                }
                if (value.f0) {
                    newRow.setField(oldRow.getArity(), 1);
                } else {
                    newRow.setField(oldRow.getArity(), -1);
                }
                out.collect(newRow);
            }
        }).setParallelism(parallelism);

        kafkaTableSink.emitDataStream(ds);
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo(org.apache.flink.table.api.Types.BOOLEAN(), new RowTypeInfo(fieldTypes, fieldNames));
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }
}
