-- Talk to elasticsearch
ADD JAR /home/user/hive/elasticsearch-hadoop-2.0.2.jar;

USE database-name;

-- Schema
CREATE EXTERNAL TABLE table-name (
	insert_time timestamp,
	record_type string,
	key string,
	value bigint
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES('es.resource' = 'hosting/table-name',
			'es.index.auto.create' = 'true',
			'es.nodes' = 'node-address1:port,node-address2:port');
