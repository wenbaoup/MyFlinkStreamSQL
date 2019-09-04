package com.yjp.flink.sql.source.kafka;


import com.yjp.flink.sql.source.IStreamSourceGener;
import com.yjp.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import com.yjp.flink.sql.table.SourceTableInfo;
import com.yjp.flink.sql.util.PluginUtil;
import com.yjp.flink.sql.util.YjpStringUtil;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * If eventtime field is specified, the default time field rowtime
 * Date: 2018/09/18
 * Company: www.dtstack.com
 *
 * @author sishu.yss
 */

public class KafkaSource implements IStreamSourceGener<Table> {

    private static final String SOURCE_OPERATOR_NAME_TPL = "${topic}_${table}";

    /**
     * Get kafka data source, you need to provide the data field names, data types
     * If you do not specify auto.offset.reset, the default use groupoffset
     *
     * @param sourceTableInfo
     * @return
     */
    @SuppressWarnings("rawtypes")
    @Override
    public Table genStreamSource(SourceTableInfo sourceTableInfo, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

        KafkaSourceTableInfo kafka011SourceTableInfo = (KafkaSourceTableInfo) sourceTableInfo;
        String topicName = kafka011SourceTableInfo.getTopic();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafka011SourceTableInfo.getBootstrapServers());
        if (YjpStringUtil.isJosn(kafka011SourceTableInfo.getOffsetReset())) {
            props.setProperty("auto.offset.reset", "none");
        } else {
            props.setProperty("auto.offset.reset", kafka011SourceTableInfo.getOffsetReset());
        }
        if (StringUtils.isNotBlank(kafka011SourceTableInfo.getGroupId())) {
            props.setProperty("group.id", kafka011SourceTableInfo.getGroupId());
        }
        // only required for Kafka 0.8
        //TODO props.setProperty("zookeeper.connect", kafka09SourceTableInfo.)

        TypeInformation[] types = new TypeInformation[kafka011SourceTableInfo.getFields().length];
        for (int i = 0; i < kafka011SourceTableInfo.getFieldClasses().length; i++) {
            types[i] = TypeInformation.of(kafka011SourceTableInfo.getFieldClasses()[i]);
        }

        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, kafka011SourceTableInfo.getFields());

        FlinkKafkaConsumer011<Row> kafkaSrc;
        if (BooleanUtils.isTrue(kafka011SourceTableInfo.getTopicIsPattern())) {
            kafkaSrc = new CustomerKafka011Consumer(Pattern.compile(topicName),
                    new CustomerJsonDeserialization(rowTypeInfo, kafka011SourceTableInfo.getPhysicalFields()), props);
        } else {
            kafkaSrc = new CustomerKafka011Consumer(topicName,
                    new CustomerJsonDeserialization(rowTypeInfo, kafka011SourceTableInfo.getPhysicalFields()), props);
        }


        //earliest,latest
        if ("earliest".equalsIgnoreCase(kafka011SourceTableInfo.getOffsetReset())) {
            kafkaSrc.setStartFromEarliest();
            // {"0":12312,"1":12321,"2":12312}
        } else if (YjpStringUtil.isJosn(kafka011SourceTableInfo.getOffsetReset())) {
            try {
                Properties properties = PluginUtil.jsonStrToObject(kafka011SourceTableInfo.getOffsetReset(), Properties.class);
                Map<String, Object> offsetMap = PluginUtil.ObjectToMap(properties);
                Map<KafkaTopicPartition, Long> specificStartupOffsets = new HashMap<>();
                for (Map.Entry<String, Object> entry : offsetMap.entrySet()) {
                    specificStartupOffsets.put(new KafkaTopicPartition(topicName, Integer.valueOf(entry.getKey())), Long.valueOf(entry.getValue().toString()));
                }
                kafkaSrc.setStartFromSpecificOffsets(specificStartupOffsets);
            } catch (Exception e) {
                throw new RuntimeException("not support offsetReset type:" + kafka011SourceTableInfo.getOffsetReset());
            }
        } else {
            kafkaSrc.setStartFromLatest();
        }

        String fields = StringUtils.join(kafka011SourceTableInfo.getFields(), ",");
        String sourceOperatorName = SOURCE_OPERATOR_NAME_TPL.replace("${topic}", topicName).replace("${table}", sourceTableInfo.getName());
        DataStreamSource<Row> kafkaSource = env.addSource(kafkaSrc, sourceOperatorName, rowTypeInfo);
        return tableEnv.fromDataStream(kafkaSource.returns(rowTypeInfo), fields);
    }
}
