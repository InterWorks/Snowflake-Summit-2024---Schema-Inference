
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
-- Ingest the CSV file

-- Create file format for CSV
create or replace file format "FF_CSV"
  type = CSV
  compression = NONE
  field_delimiter = '|'
  parse_header = TRUE
  error_on_column_count_mismatch = FALSE
;

-- Create table to store the data from CSV
create or replace table "DATA_FROM_CSV"
  using template (
    select array_agg(object_construct(*))
    from table (
      infer_schema(
          location => '@"STG__DATA"/csv'
        , file_format => '"FF_CSV"'
      )
    )
  )
  comment = 'Table containing raw data ingested from CSV file'
;

-- View table metadata
desc table "DATA_FROM_CSV";

-- Ingest CSV data using schema evolution
copy into "DATA_FROM_CSV"
from '@"STG__DATA"/csv'
  file_format = "FF_CSV"
  match_by_column_name = CASE_INSENSITIVE
;

-- View table
select * from "DATA_FROM_CSV";

-----------------------------------------
-- Ingest the JSON file

-- Create file format for JSON
create or replace file format "FF_JSON"
  type = JSON
  compression = NONE
  strip_outer_array = TRUE
;

-- Create table to store the data from JSON
create or replace table "DATA_FROM_JSON"
  using template (
    select array_agg(object_construct(*))
    from table (
      infer_schema(
          location => '@"STG__DATA"/json'
        , file_format => '"FF_JSON"'
      )
    )
  )
  comment = 'Table containing raw data ingested from JSON file'
;

-- View table metadata
desc table "DATA_FROM_JSON";

-- Ingest JSON data using schema evolution
copy into "DATA_FROM_JSON"
from '@"STG__DATA"/json'
  file_format = "FF_JSON"
  match_by_column_name = CASE_INSENSITIVE
;

-- View table
select * from "DATA_FROM_JSON";

-----------------------------------------
-- Ingest the Parquet file

-- Create file format for Parquet
create or replace file format "FF_PARQUET"
  type = PARQUET
  compression = NONE
;

-- Create table to store the data from Parquet
create or replace table "DATA_FROM_PARQUET"
  using template (
    select array_agg(object_construct(*))
    from table (
      infer_schema(
          location => '@"STG__DATA"/parquet'
        , file_format => '"FF_PARQUET"'
      )
    )
  )
  comment = 'Table containing raw data ingested from PARQUET file'
;

-- View table metadata
desc table "DATA_FROM_PARQUET";

-- Ingest Parquet data using schema evolution
copy into "DATA_FROM_PARQUET"
from '@"STG__DATA"/parquet'
  file_format = "FF_PARQUET"
  match_by_column_name = CASE_INSENSITIVE
;

-- View table
select * from "DATA_FROM_PARQUET";
