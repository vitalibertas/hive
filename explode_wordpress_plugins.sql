-- Pulling out Map Datatypes
WITH allPlugins AS (
   SELECT explode(map_values(plugins_map)) AS values
     FROM database.table
    WHERE plugins_map IS NOT NULL
      AND size(plugins_map) > 0
)
   SELECT  values
          ,COUNT(*) AS pluginCount
    FROM allPlugins
GROUP BY values
ORDER BY pluginCount DESC;
