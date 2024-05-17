
-----------------------------------------
-- Environment

use role SYSADMIN;
use warehouse "WH_GENERAL";
use database "CH_SUMMIT_2024";

-----------------------------------------
-- Reset demo

-- Recreate schema to drop all objects
create or replace schema "DEMO";
use schema "DEMO";

-- Create stage and import files from setup
create or replace stage "STG__DATA";
copy files
  into @"DEMO"."STG__DATA"
  from @"SETUP"."STG__DEMO_DATA"
;

-- Confirm files in stage
list @"STG__DATA";

-----------------------------------------
-- Create initial table

-- Create the table with only system fields
create or replace table "RAW_DATA"(
    "METADATA_FILE_PATH"                    string          comment 'Full path for the file in the originating stage'
  , "METADATA_FILE_ROW_NUMBER"              integer         comment 'Row number within the file in the originating stage'
  , "METADATA_RECORD_INGESTION_TIMESTAMP"   timestamp_ltz   comment 'Timestamp of record ingestion in local timezone'
)
  enable_schema_evolution = TRUE
  comment = 'Table containing raw data, with schema evolution enabled to automatically add new columns as required'
;

-- View table metadata
desc table "RAW_DATA";

-- View empty table
select * from "RAW_DATA";

-----------------------------------------
-- Ingest the CSV file

-- Create file format for CSV
create or replace file format "FF_CSV"
  type = CSV
  compression = NONE
  field_delimiter = '|'
  parse_header = TRUE
  error_on_column_count_mismatch = FALSE
;

-- Ingest CSV data using schema evolution
copy into "RAW_DATA"
from '@"STG__DATA"/csv'
  file_format = "FF_CSV"
  match_by_column_name = CASE_INSENSITIVE
  include_metadata = (
      "METADATA_FILE_PATH" = METADATA$FILENAME
    , "METADATA_FILE_ROW_NUMBER" = METADATA$FILE_ROW_NUMBER
    , "METADATA_RECORD_INGESTION_TIMESTAMP" = METADATA$START_SCAN_TIME
  )
;

-- View table metadata
desc table "RAW_DATA";

-- View table
select * from "RAW_DATA";

-----------------------------------------
-- Ingest the JSON file

-- Create file format for JSON
create or replace file format "FF_JSON"
  type = JSON
  compression = NONE
  strip_outer_array = TRUE
;

-- Ingest JSON data using schema evolution
copy into "RAW_DATA"
from '@"STG__DATA"/json'
  file_format = "FF_JSON"
  match_by_column_name = CASE_INSENSITIVE
  include_metadata = (
      "METADATA_FILE_PATH" = METADATA$FILENAME
    , "METADATA_FILE_ROW_NUMBER" = METADATA$FILE_ROW_NUMBER
    , "METADATA_RECORD_INGESTION_TIMESTAMP" = METADATA$START_SCAN_TIME
  )
;

-- View table metadata
desc table "RAW_DATA";

-- View table
select * from "RAW_DATA";

-----------------------------------------
-- Ingest the Parquet file

-- Create file format for Parquet
create or replace file format "FF_PARQUET"
  type = PARQUET
  compression = NONE
;

-- Ingest Parquet data using schema evolution
copy into "RAW_DATA"
from '@"STG__DATA"/parquet'
  file_format = "FF_PARQUET"
  match_by_column_name = CASE_INSENSITIVE
  include_metadata = (
      "METADATA_FILE_PATH" = METADATA$FILENAME
    , "METADATA_FILE_ROW_NUMBER" = METADATA$FILE_ROW_NUMBER
    , "METADATA_RECORD_INGESTION_TIMESTAMP" = METADATA$START_SCAN_TIME
  )
;

-- View table metadata
desc table "RAW_DATA";

-- View table
select * from "RAW_DATA";
