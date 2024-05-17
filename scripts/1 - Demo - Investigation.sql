
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
-- Review the CSV file

-- Create file format for CSV
create or replace file format "FF_CSV"
  type = CSV
  compression = NONE
  field_delimiter = '|'
  parse_header = TRUE
  error_on_column_count_mismatch = FALSE
;

-- Review inferred schema output
select *
from table(
  infer_schema(
      location => '@"STG__DATA"/csv'
    , file_format => '"FF_CSV"'
  )
)
;

-----------------------------------------
-- Review the JSON file

-- Create file format for JSON
create or replace file format "FF_JSON"
  type = JSON
  compression = NONE
  strip_outer_array = TRUE
;

-- Review inferred schema output
select *
from table(
  infer_schema(
      location => '@"STG__DATA"/json'
    , file_format => '"FF_JSON"'
  )
)
;

-----------------------------------------
-- Review the Parquet file

-- Create file format for Parquet
create or replace file format "FF_PARQUET"
  type = PARQUET
  compression = NONE
;

-- Review inferred schema output
select *
from table(
  infer_schema(
      location => '@"STG__DATA"/parquet'
    , file_format => '"FF_PARQUET"'
  )
)
;

-----------------------------------------
-- Review all files

with csv_schema as (
  select *
  from table(
    infer_schema(
        location => '@"STG__DATA"/csv'
      , file_format => '"FF_CSV"'
    )
  )
), json_schema as (
  select *
  from table(
    infer_schema(
        location => '@"STG__DATA"/json'
      , file_format => '"FF_JSON"'
    )
  )
), parquet_schema as (
  select *
  from table(
    infer_schema(
        location => '@"STG__DATA"/parquet'
      , file_format => '"FF_PARQUET"'
    )
  )
)
  select 'CSV' as source, * from csv_schema
union all
  select 'JSON' as source, * from json_schema
union all
  select 'Parquet' as source, * from parquet_schema
;
