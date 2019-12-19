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


package com.yjp.flink.sql;

import com.yjp.flink.sql.classloader.ClassLoaderManager;
import com.yjp.flink.sql.config.CalciteConfig;
import com.yjp.flink.sql.enums.ClusterMode;
import com.yjp.flink.sql.enums.ECacheType;
import com.yjp.flink.sql.environment.MyLocalStreamEnvironment;
import com.yjp.flink.sql.exec.FlinkSQLExec;
import com.yjp.flink.sql.options.OptionParser;
import com.yjp.flink.sql.options.Options;
import com.yjp.flink.sql.parser.*;
import com.yjp.flink.sql.side.SideSqlExec;
import com.yjp.flink.sql.side.SideTableInfo;
import com.yjp.flink.sql.sink.StreamSinkFactory;
import com.yjp.flink.sql.source.StreamSourceFactory;
import com.yjp.flink.sql.table.SourceTableInfo;
import com.yjp.flink.sql.table.TableInfo;
import com.yjp.flink.sql.table.TargetTableInfo;
import com.yjp.flink.sql.udf.*;
import com.yjp.flink.sql.util.FlinkUtil;
import com.yjp.flink.sql.util.PluginUtil;
import com.yjp.flink.sql.util.YjpStringUtil;
import com.yjp.flink.sql.watermarker.WaterMarkerAssigner;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.Charsets;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.calcite.shaded.com.google.common.collect.Sets;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Date: 2018/6/26
 * Company: www.yjp.com
 *
 * @author xuchao
 */

public class Main {

    private static final String CLASS_FILE_NAME_FMT = "class_path_%d";

    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    /**
     * 固定重启次数
     */
    private static final int FAILURE_NUM = 15;
    /**
     * 间隔时间 单位min
     */
    private static final int FAILURE_INTERVAL = 1;

    public static void main(String[] args) throws Exception {

        OptionParser optionParser = new OptionParser(args);
        Options options = optionParser.getOptions();
        String sql = options.getSql();
        String name = options.getName();
        String addJarListStr = options.getAddjar();
        String localSqlPluginPath = options.getLocalSqlPluginPath();
        String remoteSqlPluginPath = options.getRemoteSqlPluginPath();
        String deployMode = options.getMode();
        String confProp = options.getConfProp();
        String tableConfProp = options.getTableConfProp();

        //解码之前编码的sql
        sql = URLDecoder.decode(sql, Charsets.UTF_8.name());
        SqlParser.setLocalSqlPluginRoot(localSqlPluginPath);
        //自定义UDF使用的jar
        List<String> addJarFileList = Lists.newArrayList();
        if (!Strings.isNullOrEmpty(addJarListStr)) {
            addJarListStr = URLDecoder.decode(addJarListStr, Charsets.UTF_8.name());
            addJarFileList = OBJ_MAPPER.readValue(addJarListStr, List.class);
        }

        //StreamExecutionEnvironment 配置
        confProp = URLDecoder.decode(confProp, Charsets.UTF_8.toString());
        Properties confProperties = PluginUtil.jsonStrToObject(confProp, Properties.class);
        //根据配置文件设置env
        StreamExecutionEnvironment env = getStreamExeEnv(confProperties, deployMode);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //StreamTableEnvironment 配置
        if (tableConfProp != null) {
            tableConfProp = URLDecoder.decode(tableConfProp, Charsets.UTF_8.toString());
            Properties tableConfProperties = PluginUtil.jsonStrToObject(tableConfProp, Properties.class);
            //配置状态过期时间
            StreamQueryConfig qConfig = tableEnv.queryConfig();
            //这里暂时只设置过期时间 后期需要修改本方法
            setTableConfig(qConfig, tableConfProperties);
        }

        List<URL> jarURList = Lists.newArrayList();
        //解析sql
        SqlTree sqlTree = SqlParser.parseSql(sql);

        //Get External jar to load
        for (String addJarPath : addJarFileList) {
            File tmpFile = new File(addJarPath);
            jarURList.add(tmpFile.toURI().toURL());
        }

        Map<String, SideTableInfo> sideTableMap = Maps.newHashMap();
        Map<String, Table> registerTableCache = Maps.newHashMap();

        //register udf
        registerUDF(sqlTree, jarURList, tableEnv);
        tableEnv.registerFunction("date_convert", new DateFormatUDF());
        tableEnv.registerFunction("dateToLong", new DateToLongUDF());
        tableEnv.registerFunction("if_null", new IfNullUDF());
        tableEnv.registerFunction("TimestampToLong", new TimestampToLongUdf());
        tableEnv.registerFunction("string_is_not_null", new StringIsNotNullUDF());

        //register table schema
        registerTable(sqlTree, env, tableEnv, localSqlPluginPath, remoteSqlPluginPath, sideTableMap, registerTableCache);


        sqlTranslation(localSqlPluginPath, tableEnv, sqlTree, sideTableMap, registerTableCache);

        if (env instanceof MyLocalStreamEnvironment) {
            ((MyLocalStreamEnvironment) env).setClasspaths(ClassLoaderManager.getClassPath());
        }

        env.execute(name);
        System.out.println("main 方法结束");
    }

    private static void setTableConfig(StreamQueryConfig qConfig, Properties tableConfProperties) {
        qConfig.withIdleStateRetentionTime(
                Time.hours(Long.parseLong(tableConfProperties.getProperty("table.conf.idlestateretentiontime.min"))),
                Time.hours(Long.parseLong(tableConfProperties.getProperty("table.conf.idlestateretentiontime.max"))));
    }


    private static void sqlTranslation(String localSqlPluginPath, StreamTableEnvironment tableEnv, SqlTree sqlTree, Map<String, SideTableInfo> sideTableMap, Map<String, Table> registerTableCache) throws Exception {
        SideSqlExec sideSqlExec = new SideSqlExec();
        sideSqlExec.setLocalSqlPluginPath(localSqlPluginPath);
        for (CreateTmpTableParser.SqlParserResult result : sqlTree.getTmpSqlList()) {
            sideSqlExec.registerTmpTable(result, sideTableMap, tableEnv, registerTableCache);
        }

        for (InsertSqlParser.SqlParseResult result : sqlTree.getExecSqlList()) {
            if (LOG.isInfoEnabled()) {
                LOG.info("exe-sql:\n" + result.getExecSql());
            }
            boolean isSide = false;
            for (String tableName : result.getTargetTableList()) {
                if (sqlTree.getTmpTableMap().containsKey(tableName)) {
                    CreateTmpTableParser.SqlParserResult tmp = sqlTree.getTmpTableMap().get(tableName);
                    String realSql = YjpStringUtil.replaceIgnoreQuota(result.getExecSql(), "`", "");

                    SqlNode sqlNode = org.apache.calcite.sql.parser.SqlParser.create(realSql, CalciteConfig.MYSQL_LEX_CONFIG).parseStmt();
                    String tmpSql = ((SqlInsert) sqlNode).getSource().toString();
                    tmp.setExecSql(tmpSql);
                    sideSqlExec.registerTmpTable(tmp, sideTableMap, tableEnv, registerTableCache);
                } else {
                    for (String sourceTable : result.getSourceTableList()) {
                        if (sideTableMap.containsKey(sourceTable)) {
                            isSide = true;
                            break;
                        }
                    }
                    if (isSide) {
                        //sql-dimensional table contains the dimension table of execution
                        sideSqlExec.exec(result.getExecSql(), sideTableMap, tableEnv, registerTableCache);
                    } else {
                        FlinkSQLExec.sqlUpdate(tableEnv, result.getExecSql());
                        if (LOG.isInfoEnabled()) {
                            LOG.info("exec sql: " + result.getExecSql());
                        }
                    }
                }
            }
        }


    }

    /**
     * This part is just to add classpath for the jar when reading remote execution, and will not submit jar from a local
     *
     * @param env
     * @param classPathSet
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    private static void addEnvClassPath(StreamExecutionEnvironment env, Set<URL> classPathSet) throws NoSuchFieldException, IllegalAccessException {
        if (env instanceof StreamContextEnvironment) {
            Field field = env.getClass().getDeclaredField("ctx");
            field.setAccessible(true);
            ContextEnvironment contextEnvironment = (ContextEnvironment) field.get(env);
            for (URL url : classPathSet) {
                contextEnvironment.getClasspaths().add(url);
            }
        }
    }

    private static void registerUDF(SqlTree sqlTree, List<URL> jarURList, StreamTableEnvironment tableEnv)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        //register urf
        // udf和tableEnv须由同一个类加载器加载
        ClassLoader levelClassLoader = tableEnv.getClass().getClassLoader();
        URLClassLoader classLoader = null;
        List<CreateFuncParser.SqlParserResult> funcList = sqlTree.getFunctionList();
        for (CreateFuncParser.SqlParserResult funcInfo : funcList) {
            //classloader
            if (classLoader == null) {
                classLoader = FlinkUtil.loadExtraJar(jarURList, (URLClassLoader) levelClassLoader);
            }
            FlinkUtil.registerUDF(funcInfo.getType(), funcInfo.getClassName(), funcInfo.getName(), tableEnv, classLoader);
        }
    }


    private static void registerTable(SqlTree sqlTree, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv,
                                      String localSqlPluginPath, String remoteSqlPluginPath,
                                      Map<String, SideTableInfo> sideTableMap, Map<String, Table> registerTableCache) throws Exception {
        Set<URL> classPathSet = Sets.newHashSet();
        WaterMarkerAssigner waterMarkerAssigner = new WaterMarkerAssigner();
        for (TableInfo tableInfo : sqlTree.getTableInfoMap().values()) {

            if (tableInfo instanceof SourceTableInfo) {

                SourceTableInfo sourceTableInfo = (SourceTableInfo) tableInfo;
                Table table = StreamSourceFactory.getStreamSource(sourceTableInfo, env, tableEnv, localSqlPluginPath);
                tableEnv.registerTable(sourceTableInfo.getAdaptName(), table);
                //Note --- parameter conversion function can not be used inside a function of the type of polymerization
                //Create table in which the function is arranged only need adaptation sql
                String adaptSql = sourceTableInfo.getAdaptSelectSql();
                Table adaptTable = adaptSql == null ? table : tableEnv.sqlQuery(adaptSql);

                RowTypeInfo typeInfo = new RowTypeInfo(adaptTable.getSchema().getFieldTypes(), adaptTable.getSchema().getFieldNames());
                DataStream adaptStream = tableEnv.toRetractStream(adaptTable, typeInfo)
                        .map((Tuple2<Boolean, Row> f0) -> f0.f1)
                        .returns(typeInfo);

                String fields = String.join(",", typeInfo.getFieldNames());

                if (waterMarkerAssigner.checkNeedAssignWaterMarker(sourceTableInfo)) {
                    adaptStream = waterMarkerAssigner.assignWaterMarker(adaptStream, typeInfo, sourceTableInfo);
                    fields += ",ROWTIME.ROWTIME";
                } else {
                    fields += ",PROCTIME.PROCTIME";
                }

                Table regTable = tableEnv.fromDataStream(adaptStream, fields);
                tableEnv.registerTable(tableInfo.getName(), regTable);
                if (LOG.isInfoEnabled()) {
                    LOG.info("registe table {} success.", tableInfo.getName());
                }
                registerTableCache.put(tableInfo.getName(), regTable);
                classPathSet.add(PluginUtil.getRemoteJarFilePath(tableInfo.getType(), SourceTableInfo.SOURCE_SUFFIX, remoteSqlPluginPath, localSqlPluginPath));
            } else if (tableInfo instanceof TargetTableInfo) {

                TableSink tableSink = StreamSinkFactory.getTableSink((TargetTableInfo) tableInfo, localSqlPluginPath);
                TypeInformation[] flinkTypes = FlinkUtil.transformTypes(tableInfo.getFieldClasses());
                tableEnv.registerTableSink(tableInfo.getName(), tableInfo.getFields(), flinkTypes, tableSink);
                classPathSet.add(PluginUtil.getRemoteJarFilePath(tableInfo.getType(), TargetTableInfo.TARGET_SUFFIX, remoteSqlPluginPath, localSqlPluginPath));
            } else if (tableInfo instanceof SideTableInfo) {

                String sideOperator = ECacheType.ALL.name().equals(((SideTableInfo) tableInfo).getCacheType()) ? "all" : "async";
                sideTableMap.put(tableInfo.getName(), (SideTableInfo) tableInfo);
                classPathSet.add(PluginUtil.getRemoteSideJarFilePath(tableInfo.getType(), sideOperator, SideTableInfo.TARGET_SUFFIX, remoteSqlPluginPath, localSqlPluginPath));
            } else {
                throw new RuntimeException("not support table type:" + tableInfo.getType());
            }
        }

        //The plug-in information corresponding to the table is loaded into the classPath env
        addEnvClassPath(env, classPathSet);
        int i = 0;
        for (URL url : classPathSet) {
            String classFileName = String.format(CLASS_FILE_NAME_FMT, i);
            env.registerCachedFile(url.getPath(), classFileName, true);
            i++;
        }
    }

    private static StreamExecutionEnvironment getStreamExeEnv(Properties confProperties, String deployMode) throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        StreamExecutionEnvironment env = !ClusterMode.local.name().equals(deployMode) ?
                StreamExecutionEnvironment.getExecutionEnvironment() :
                new MyLocalStreamEnvironment();

        env.setParallelism(FlinkUtil.getEnvParallelism(confProperties));
        Configuration globalJobParameters = new Configuration();
        //本质是就是将confProperties中的数据变为了HashMap然后传入Configuration
        Method method = Configuration.class.getDeclaredMethod("setValueInternal", String.class, Object.class);
        method.setAccessible(true);

        for (Map.Entry<Object, Object> prop : confProperties.entrySet()) {
            method.invoke(globalJobParameters, prop.getKey(), prop.getValue());
        }

        ExecutionConfig exeConfig = env.getConfig();
        //env中是否设置过全局变量
        if (exeConfig.getGlobalJobParameters() == null) {
            exeConfig.setGlobalJobParameters(globalJobParameters);
        } else if (exeConfig.getGlobalJobParameters() instanceof Configuration) {
            ((Configuration) exeConfig.getGlobalJobParameters()).addAll(globalJobParameters);
        }


        if (FlinkUtil.getMaxEnvParallelism(confProperties) > 0) {
            env.setMaxParallelism(FlinkUtil.getMaxEnvParallelism(confProperties));
        }

        if (FlinkUtil.getBufferTimeoutMillis(confProperties) > 0) {
            env.setBufferTimeout(FlinkUtil.getBufferTimeoutMillis(confProperties));
        }
        //配置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                FAILURE_NUM,
                Time.of(FAILURE_INTERVAL, TimeUnit.MINUTES)
        ));
        //设置时间策略  默认process time
        FlinkUtil.setStreamTimeCharacteristic(env, confProperties);
        //配置checkPoint
        FlinkUtil.openCheckpoint(env, confProperties);

        return env;
    }
}
