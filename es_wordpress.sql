-- Talk to elasticsearch
ADD JAR /home/user/hive/elasticsearch-hadoop-2.0.2.jar;

USE database-name;

SET hive.execution.engine                = tez;

-- Wordpress instances by product
INSERT INTO TABLE es_stats
   SELECT  '${hiveconf:START_DATE} 00:00:00' AS insert_time
          ,'instances-by-product' AS record_type
          ,product AS key
          ,COUNT(*) AS value
     FROM table-name
    WHERE event_time >= to_date('${hiveconf:START_DATE}')
      AND event_time < to_date('${hiveconf:END_DATE}')
      AND job_id = regexp_replace(to_date('${hiveconf:START_DATE}'),"-","")
 GROUP BY product
 ORDER BY product
;

-- Wordpress instances delta by product
WITH cte_previous AS (
   SELECT  product
          ,COUNT(*) AS instances
     FROM table-name
	WHERE event_time >= date_sub(to_date('${hiveconf:START_DATE}'), 1)
      AND event_time <  date_sub(to_date('${hiveconf:END_DATE}'), 1)
	  AND job_id = regexp_replace(date_sub(to_date('${hiveconf:START_DATE}'),1),"-","")
 GROUP BY product
), cte_current AS (
	SELECT  product
           ,COUNT(*) AS instances
	FROM table-name
	WHERE event_time >= to_date('${hiveconf:START_DATE}')
	AND event_time <  to_date('${hiveconf:END_DATE}')
	AND job_id = regexp_replace(to_date('${hiveconf:START_DATE}'),"-","")
	GROUP BY product
)
INSERT INTO TABLE es_stats
   SELECT  '${hiveconf:START_DATE} 00:00:00' AS insert_time
          ,'instance-delta-by-product' AS record_type
          ,now.product AS key
          ,COALESCE(now.instances, 0) - COALESCE(prev.instances, 0) AS value
     FROM cte_current AS now
     JOIN cte_previous AS prev
	      ON prev.product = now.product
 ORDER BY key
;

-- Wordpress instances by product by version
WITH cte_instances AS (
    SELECT  product
	       ,version
           ,COUNT(*) AS instances
      FROM table-name
     WHERE event_time >= to_date('${hiveconf:START_DATE}')
       AND event_time < to_date('${hiveconf:END_DATE}')
       AND job_id = regexp_replace(to_date('${hiveconf:START_DATE}'),"-","")
  GROUP BY  product
           ,version
)
INSERT INTO TABLE es_stats
    SELECT  '${hiveconf:START_DATE} 00:00:00' AS insert_time
           ,CONCAT('instances-by-version-', product) AS record_type
           ,version AS key
           ,instances AS value
      FROM cte_instances
  ORDER BY key
;
