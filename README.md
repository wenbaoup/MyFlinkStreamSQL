# YjpFlinkStreamSQL

依据袋鼠云的flinkStreamSQL升级1.9版本 新增RocksDb后端储存以及Sql state的ttl配置

启动参数 

shell 启动 

sh submit.sh -sql /data/start-stream-sql/sideSql_1.txt -name xctest -remoteSqlPluginPath /data/stream-sql/plugins -localSqlPluginPath /data/stream-sql/plugins -mode yarn -flinkconf /data/flink-1.9.0/conf  -yarnconf /etc/hadoop/conf -confProp \{\"sql.checkpoint.interval\":300000,\"sql.checkpoint.mode\":\"EXACTLY_ONCE\",\"sql.checkpoint.timeout\":3600000,\"sql.max.concurrent.checkpoints\":1,\"flinkCheckpointDataURI\":\"hdfs://nameservice1/flinkcheckpoints\",\"sql.checkpoint.state.backend.mode\":\"RocksDBStateBackend\"\} 

shell 启动  tableConfProp

sh submit.sh -sql /data/start-stream-sql/sideSql_1.txt -name xctest -remoteSqlPluginPath /data/stream-sql/plugins -localSqlPluginPath /data/stream-sql/plugins -mode yarn -flinkconf /data/flink-1.9.0/conf  -yarnconf /etc/hadoop/conf -confProp \{\"sql.checkpoint.interval\":300000,\"sql.checkpoint.mode\":\"EXACTLY_ONCE\",\"sql.checkpoint.timeout\":3600000,\"sql.max.concurrent.checkpoints\":1,\"flinkCheckpointDataURI\":\"hdfs://nameservice1/flinkcheckpoints\",\"sql.checkpoint.state.backend.mode\":\"RocksDBStateBackend\"\} -tableConfProp \{\"table.conf.idlestateretentiontime.min\":24,\"table.conf.idlestateretentiontime.max\":36\}

idea启动 local

sh submit.sh -sql D:\sideSql_1.txt -name xctest -remoteSqlPluginPath E:\Users\my_git\YjpFlinkStreamSQL\plugins -localSqlPluginPath E:\Users\my_git\YjpFlinkStreamSQL\plugins -mode local  -confProp {}
