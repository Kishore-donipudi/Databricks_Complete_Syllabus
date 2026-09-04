CREATE OR REFRESH STREAMING TABLE sql_stream
AS
SELECT * FROM STREAM(read_files(
    '/Volumes/kishore_d/default/csv',
    format => 'csv',
    header => true,
    inferSchema => true
));

/*
CREATE LIVE TABLE sql_stream
AS SELECT * FROM cloud_files(
  '/Volumes/kishore_d/default/csv',
  'csv',
  OPTIONS('header' = 'true', 'inferSchema' = 'true')
);
*/

CREATE OR REFRESH MATERIALIZED VIEW sql_stream_mv
AS
SELECT count(*) AS total FROM sql_stream;

/*
CREATE LIVE MATERIALIZED VIEW → declares a materialized view in your pipeline.

IF NOT EXISTS → ensures it won’t error if already created.

LIVE.sql_stream → references the live table defined earlier in the pipeline. 
*/

create or refresh materialized view sql_st_01
as select count(*)/2  as cnt2 from food_sales_view ; 