/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yjp.flink.sql.launcher;

import com.yjp.flink.sql.enums.ClusterMode;
import com.yjp.flink.sql.options.Options;
import com.yjp.flink.sql.util.PluginUtil;
import com.yjp.flink.sql.yarn.JobParameter;
import com.yjp.flink.sql.yarn.YarnClusterConfiguration;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.client.deployment.StandaloneClusterDescriptor;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.StringHelper;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * The Factory of ClusterClient
 * <p>
 * Company: www.yjp.com
 *
 * @author huyifan.zju@163.com
 */
public class ClusterClientFactory {

    public static final String FLINK_DIST = "flink-dist";

    public static ClusterClient createClusterClient(Options launcherOptions) throws Exception {
        String mode = launcherOptions.getMode();
        if (mode.equals(ClusterMode.standalone.name())) {
            return createStandaloneClient(launcherOptions);
        } else if (mode.equals(ClusterMode.yarn.name())) {
            return createYarnClient(launcherOptions, mode);
        }

        throw new IllegalArgumentException("Unsupported cluster client type: ");
    }

    public static ClusterClient createStandaloneClient(Options launcherOptions) throws Exception {
        String flinkConfDir = launcherOptions.getFlinkconf();
        org.apache.flink.configuration.Configuration config = GlobalConfiguration.loadConfiguration(flinkConfDir);

        StandaloneClusterDescriptor standaloneClusterDescriptor = new StandaloneClusterDescriptor(config);
        RestClusterClient clusterClient = standaloneClusterDescriptor.retrieve(StandaloneClusterId.getInstance());

        LeaderConnectionInfo connectionInfo = clusterClient.getClusterConnectionInfo();
        InetSocketAddress address = AkkaUtils.getInetSocketAddressFromAkkaURL(connectionInfo.getAddress());
        config.setString(JobManagerOptions.ADDRESS, address.getAddress().getHostName());
        config.setInteger(JobManagerOptions.PORT, address.getPort());
        clusterClient.setDetached(true);
        return clusterClient;
    }

    public static ClusterClient createYarnClient(Options launcherOptions, String mode) {
        String flinkConfDir = launcherOptions.getFlinkconf();
        org.apache.flink.configuration.Configuration flinkConf = GlobalConfiguration.loadConfiguration(flinkConfDir);
        String yarnConfDir = launcherOptions.getYarnconf();
        YarnConfiguration yarnConf;
        if (StringUtils.isNotBlank(yarnConfDir)) {

            try {
                flinkConf.setString(ConfigConstants.PATH_HADOOP_CONFIG, yarnConfDir);
                FileSystem.initialize(flinkConf);

                File dir = new File(yarnConfDir);
                if (dir.exists() && dir.isDirectory()) {
                    yarnConf = loadYarnConfiguration(yarnConfDir);

                    YarnClient yarnClient = YarnClient.createYarnClient();
                    YarnConfLoader.haYarnConf(yarnConf);
                    yarnClient.init(yarnConf);
                    yarnClient.start();

                    String confProp = launcherOptions.getConfProp();
                    confProp = URLDecoder.decode(confProp, Charsets.UTF_8.toString());
                    System.out.println("confProp=" + confProp);
                    Properties confProperties = PluginUtil.jsonStrToObject(confProp, Properties.class);

                    ApplicationId applicationId;
                    ClusterClient clusterClient;
                    //on yarn cluster mode
                    if (mode.equals(ClusterMode.yarn.name())) {
                        String yarnSessionConf = launcherOptions.getYarnSessionConf();
                        yarnSessionConf = URLDecoder.decode(yarnSessionConf, Charsets.UTF_8.toString());
                        Properties yarnSessionConfProperties = PluginUtil.jsonStrToObject(yarnSessionConf, Properties.class);
                        String yid = null;
                        String yName = null;
                        if (null != yarnSessionConfProperties.get("yid")) {
                            yid = yarnSessionConfProperties.get("yid").toString();
                        }
                        if (null != yarnSessionConfProperties.get("yname")) {
                            yName = yarnSessionConfProperties.get("yname").toString();
                        }
                        if (StringUtils.isNotBlank(yid)) {
                            applicationId = toApplicationId(yid);
                        } else if (StringUtils.isNotBlank(yName)) {
                            applicationId = getYarnClusterApplicationId(yarnClient, yName);
                        } else {
                            applicationId = getYarnClusterApplicationId(yarnClient, "Flink session");
                        }
                        System.out.println("applicationId=" + applicationId.toString());

                        AbstractYarnClusterDescriptor clusterDescriptor = new YarnClusterDescriptor(
                                flinkConf, yarnConf, ".", yarnClient, false);
                        clusterClient = clusterDescriptor.retrieve(applicationId);

                        System.out.println("applicationId=" + applicationId.toString() + " has retrieve!");
                    } else {//on yarn per-job mode
                        applicationId = createApplication(yarnClient);
                        System.out.println("applicationId=" + applicationId.toString());

                        YarnClusterConfiguration clusterConf = getYarnClusterConfiguration(flinkConf, yarnConf, flinkConfDir);
                        //jobmanager+taskmanager param
                        JobParameter appConf = new JobParameter(confProperties);

                        com.yjp.flink.sql.yarn.YarnClusterDescriptor clusterDescriptor = new com.yjp.flink.sql.yarn.YarnClusterDescriptor(
                                clusterConf, yarnClient, appConf, applicationId, launcherOptions.getName(), null);
                        clusterClient = clusterDescriptor.deploy();

                        System.out.println("applicationId=" + applicationId.toString() + " has deploy!");
                    }
                    clusterClient.setDetached(true);
                    yarnClient.stop();
                    return clusterClient;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        throw new UnsupportedOperationException("Haven't been developed yet!");
    }

    private static YarnConfiguration loadYarnConfiguration(String yarnConfDir) {
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        Stream.of("yarn-site.xml", "core-site.xml", "hdfs-site.xml").forEach(file -> {
            File site = new File(requireNonNull(yarnConfDir, "ENV HADOOP_CONF_DIR is not setting"), file);
            if (site.exists() && site.isFile()) {
                hadoopConf.addResource(new org.apache.hadoop.fs.Path(site.toURI()));
            } else {
                throw new RuntimeException(site + " not exists");
            }
        });

        return new YarnConfiguration(hadoopConf);
    }

    public static YarnClusterConfiguration getYarnClusterConfiguration(Configuration flinkConf, YarnConfiguration yarnConf, String flinkConfDir) {
        Path flinkJar = new Path(getFlinkJarFile(flinkConfDir).toURI());
        @SuppressWarnings("ConstantConditions") final Set<Path> resourcesToLocalize = Stream
                .of("flink-conf.yaml", "log4j.properties")
                .map(x -> new Path(new File(flinkConfDir, x).toURI()))
                .collect(Collectors.toSet());

        return new YarnClusterConfiguration(flinkConf, yarnConf, "", flinkJar, resourcesToLocalize);
    }


    private static File getFlinkJarFile(String flinkConfDir) {
        String errorMessage = "error not search " + FLINK_DIST + "*.jar";
        File[] files = requireNonNull(new File(flinkConfDir, "/../lib").listFiles(), errorMessage);
        Optional<File> file = Arrays.stream(files)
                .filter(f -> f.getName().startsWith(FLINK_DIST)).findFirst();
        return file.orElseThrow(() -> new IllegalArgumentException(errorMessage));
    }

    private static ApplicationId createApplication(YarnClient yarnClient) throws IOException, YarnException {
        YarnClientApplication app = yarnClient.createApplication();
        return app.getApplicationSubmissionContext().getApplicationId();
    }

    private static ApplicationId getYarnClusterApplicationId(YarnClient yarnClient, String yName) throws Exception {
        ApplicationId applicationId = null;

        Set<String> set = new HashSet<>();
        set.add("Apache Flink");
        EnumSet<YarnApplicationState> enumSet = EnumSet.noneOf(YarnApplicationState.class);
        enumSet.add(YarnApplicationState.RUNNING);
        List<ApplicationReport> reportList = yarnClient.getApplications(set, enumSet);

        int maxMemory = -1;
        int maxCores = -1;
        for (ApplicationReport report : reportList) {
            if (!report.getName().startsWith(yName)) {
                continue;
            }

            if (!report.getYarnApplicationState().equals(YarnApplicationState.RUNNING)) {
                continue;
            }

            int thisMemory = report.getApplicationResourceUsageReport().getNeededResources().getMemory();
            int thisCores = report.getApplicationResourceUsageReport().getNeededResources().getVirtualCores();
            boolean flag = thisMemory > maxMemory || thisMemory == maxMemory && thisCores > maxCores;
            if (flag) {
                maxMemory = thisMemory;
                maxCores = thisCores;
                applicationId = report.getApplicationId();
            }

        }

        if (StringUtils.isEmpty(applicationId != null ? applicationId.toString() : null)) {
            throw new RuntimeException("No flink session found on yarn cluster.");
        }
        return applicationId;
    }


    private static ApplicationId toApplicationId(String appIdStr) {
        Iterator<String> it = StringHelper._split(appIdStr).iterator();
        if (!"application".equals(it.next())) {
            throw new IllegalArgumentException("Invalid ApplicationId prefix: " + appIdStr + ". The valid ApplicationId should start with prefix " + "application");
        } else {
            try {
                return toApplicationId(it);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid AppAttemptId: " + appIdStr, e);
            }
        }
    }

    private static ApplicationId toApplicationId(Iterator<String> it) throws NumberFormatException {
        return ApplicationId.newInstance(Long.parseLong(it.next()), Integer.parseInt(it.next()));
    }
}
