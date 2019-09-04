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


package com.yjp.flink.sql.source;


import com.yjp.flink.sql.classloader.YjpClassLoader;
import com.yjp.flink.sql.table.AbsSourceParser;
import com.yjp.flink.sql.table.SourceTableInfo;
import com.yjp.flink.sql.util.PluginUtil;
import com.yjp.flink.sql.util.YjpStringUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * streamTableSource
 * Date: 2017/3/10
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class StreamSourceFactory {

    private static final String CURR_TYPE = "source";

    private static final String DIR_NAME_FORMAT = "%ssource";

    public static AbsSourceParser getSqlParser(String pluginType, String sqlRootDir) throws Exception {

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        //补充remoteSqlPluginPath的路径 确定加载的路径
        String pluginJarPath = PluginUtil.getJarFileDirPath(String.format(DIR_NAME_FORMAT, pluginType), sqlRootDir);

        YjpClassLoader dtClassLoader = (YjpClassLoader) classLoader;
        //通过绝对路径加载需要的Jar
        PluginUtil.addPluginJar(pluginJarPath, dtClassLoader);
        //typeNoVersion 去掉版本 如kafka11  typeNoVersion=kafka
        String typeNoVersion = YjpStringUtil.getPluginTypeWithoutVersion(pluginType);
        //得到KafkaSourceParser 的类名
        String className = PluginUtil.getSqlParserClassName(typeNoVersion, CURR_TYPE);
        //获取子类的CLass对象
        Class<?> sourceParser = dtClassLoader.loadClass(className);
        if (!AbsSourceParser.class.isAssignableFrom(sourceParser)) {
            throw new RuntimeException("class " + sourceParser.getName() + " not subClass of AbsSourceParser");
        }

        return sourceParser.asSubclass(AbsSourceParser.class).newInstance();
    }

    /**
     * The configuration of the type specified data source
     *
     * @param sourceTableInfo
     * @return
     */
    public static Table getStreamSource(SourceTableInfo sourceTableInfo, StreamExecutionEnvironment env,
                                        StreamTableEnvironment tableEnv, String sqlRootDir) throws Exception {

        String sourceTypeStr = sourceTableInfo.getType();
        String typeNoVersion = YjpStringUtil.getPluginTypeWithoutVersion(sourceTypeStr);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        String pluginJarPath = PluginUtil.getJarFileDirPath(String.format(DIR_NAME_FORMAT, sourceTypeStr), sqlRootDir);
        String className = PluginUtil.getGenerClassName(typeNoVersion, CURR_TYPE);

        YjpClassLoader dtClassLoader = (YjpClassLoader) classLoader;
        PluginUtil.addPluginJar(pluginJarPath, dtClassLoader);
        Class<?> sourceClass = dtClassLoader.loadClass(className);

        if (!IStreamSourceGener.class.isAssignableFrom(sourceClass)) {
            throw new RuntimeException("class " + sourceClass.getName() + " not subClass of IStreamSourceGener");
        }

        IStreamSourceGener sourceGener = sourceClass.asSubclass(IStreamSourceGener.class).newInstance();
        Object object = sourceGener.genStreamSource(sourceTableInfo, env, tableEnv);
        return (Table) object;
    }
}
