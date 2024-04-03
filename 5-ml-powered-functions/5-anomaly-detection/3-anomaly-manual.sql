USE SCHEMA anomaly.public;

-- for first store only, unlabeled/unsupervized
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION model1(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT date, sales FROM view1_train'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  LABEL_COLNAME => '');           -- '' for unlabeled/unsupervized
SHOW SNOWFLAKE.ML.ANOMALY_DETECTION;

CALL model1!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT date, sales FROM view1_test'),
  TIMESTAMP_COLNAME =>'date',
  TARGET_COLNAME => 'sales');

-- for second store only, unlabeled/unsupervized
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION model2(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT date, sales FROM view2_train'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  LABEL_COLNAME => '');
SHOW SNOWFLAKE.ML.ANOMALY_DETECTION;

CALL model2!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT date, sales FROM view2_test'),
  TIMESTAMP_COLNAME =>'date',
  TARGET_COLNAME => 'sales');

CALL model2!EXPLAIN_FEATURE_IMPORTANCE();

-- for first store only, labeled/supervized, w/ all additional columns
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION model1l(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view1_train'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  LABEL_COLNAME => 'outlier');       -- column name w/ TRUE/FALSE labels, training data only

CALL model1l!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view1_test'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales');

CALL model1l!EXPLAIN_FEATURE_IMPORTANCE();

-- for all data, multiple time-series
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION model(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_train'),
  SERIES_COLNAME => 'store_item',     -- [store_item, item] multiple TS
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  LABEL_COLNAME => 'outlier');

CALL model!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_test'),
  SERIES_COLNAME => 'store_item',
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  CONFIG_OBJECT => {'prediction_interval':0.995});

CALL model!EXPLAIN_FEATURE_IMPORTANCE();
  
-- emulate error w/ duplicate entry, to show in logs
CALL model!SHOW_TRAINING_LOGS();

INSERT INTO sales_table VALUES
  (1, 'jacket', to_timestamp_ntz('2020-01-03'), 5.0, false, 54, 0.2, 'duplicate');

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION model_err(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_train'),
  SERIES_COLNAME => 'store_item',
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  LABEL_COLNAME => 'outlier',
  CONFIG_OBJECT => {'ON_ERROR': 'SKIP'});
CALL model_err!SHOW_TRAINING_LOGS();

DELETE FROM sales_table
WHERE holiday = 'duplicate';
