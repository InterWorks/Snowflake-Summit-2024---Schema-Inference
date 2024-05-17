
-----------------------------------------
-- Environment

use role SYSADMIN;

-----------------------------------------
-- Warehouse

create warehouse if not exists "WH_GENERAL"
  warehouse_size = 'XSMALL'
  comment = 'General usage warehouse'
  initially_suspended = TRUE -- Do not start the warehouse immediately. Instead, wait until it is used by another query
;

use warehouse "WH_GENERAL";

-----------------------------------------
-- Generate Setup Schema

create or replace database "CH_SUMMIT_2024";
use database "CH_SUMMIT_2024";

create or replace schema "SETUP";
use schema "SETUP";

-----------------------------------------
-- Generate Demo Data

-- Create demo data table
create or replace table "DEMO_DATA"(
    "EVENT_ID"                string        not null
  , "EVENT_GROUP"             integer       not null
  , "EVENT_DATE"              date
  , "EVENT_TIME"              time
  , "EVENT_TIMESTAMP"         timestamp
  , "EVENT_CATEGORY"          string
  , "EVENT_NAME"              string
  , "EVENT_DESCRIPTION"       string
  , "EVENT_LINKS"             variant
)
as
select
    uuid_string() as "EVENT_ID"
  , $1 as "EVENT_GROUP"
  , '2024-06-0' || "EVENT_GROUP"::string as "EVENT_DATE"
  , $2::time as "EVENT_TIME"
  , timestamp_from_parts("EVENT_DATE", "EVENT_TIME") as "EVENT_TIMESTAMP"
  , $3 as "EVENT_CATEGORY"
  , $4 as "EVENT_NAME"
  , $5 as "EVENT_DESCRIPTION"
  , object_construct(
        'servers'
        , array_construct(
              uuid_string()  
            , uuid_string()  
            , uuid_string()  
          )
      , 'activities'
        , array_construct(
              uuid_string()  
            , uuid_string()  
            , uuid_string()  
          )
    ) as "EVENT_LINKS"
from values
    (1, '08:15', 'VIRTUAL MACHINE', 'VM start', 'Virtual machine has started')
  , (1, '08:20', 'APPLICATION', 'App start', 'Application has started')
  , (1, '08:25', 'APPLICATION', 'App error', 'Application encountered an error. View error log for details')
  , (1, '08:30', 'VIRTUAL MACHINE', 'VM stopped', 'Virtual machine has stopped')

  , (2, '08:15', 'VIRTUAL MACHINE', 'VM start', 'Virtual machine has started')
  , (2, '08:20', 'APPLICATION', 'App start', 'Application has started')
  , (2, '08:25', 'APPLICATION', 'App heartbeat', 'Application heartbeat successful')
  , (2, '08:30', 'APPLICATION', 'App heartbeat', 'Application heartbeat successful')
  , (2, '08:35', 'APPLICATION', 'App heartbeat', 'Application heartbeat successful')
  , (2, '08:40', 'APPLICATION', 'App heartbeat', 'Application heartbeat successful')

  , (3, '08:15', 'VIRTUAL MACHINE', 'VM start', 'Virtual machine has started')
  , (3, '08:20', 'APPLICATION', 'App start', 'Application has started')
  , (3, '08:25', 'APPLICATION', 'App error', 'Application encountered an error. View error log for details')
  , (3, '08:30', 'VIRTUAL MACHINE', 'VM stopped', 'Virtual machine has stopped')
;

-- View demo date
select * from "DEMO_DATA";

-----------------------------------------
-- Unload Demo Data

-- Create stage to store demo data
create or replace stage "STG__DEMO_DATA";

-- Generate CSV
copy into @"STG__DEMO_DATA"/csv/1.csv
from (
  select
      "EVENT_ID"
    , "EVENT_GROUP"
    , "EVENT_DATE"
    , "EVENT_TIME"
    -- , "EVENT_TIMESTAMP"
    -- , "EVENT_CATEGORY"
    , "EVENT_NAME"
    , "EVENT_DESCRIPTION"
    -- , "EVENT_LINKS"
  from "DEMO_DATA"
  where "EVENT_GROUP" = 1
)
file_format = (type = CSV compression = NONE field_delimiter = '|')
header = TRUE
single = TRUE
;

-- Generate JSON
copy into @"STG__DEMO_DATA"/json/2.json
from (
  select array_agg(object_construct(*)) as "RAW_JSON"
  from (
    select
        "EVENT_ID"
      , "EVENT_GROUP"
      -- , "EVENT_DATE"
      -- , "EVENT_TIME"
      , "EVENT_TIMESTAMP"
      -- , "EVENT_CATEGORY"
      , "EVENT_NAME"
      , "EVENT_DESCRIPTION"
      , "EVENT_LINKS"
    from "DEMO_DATA"
    where "EVENT_GROUP" = 2
  )
)
file_format = (type = JSON compression = NONE)
header = FALSE
single = TRUE
;

-- Generate parquet
copy into @"STG__DEMO_DATA"/parquet/3.parquet
from (
  select
      "EVENT_ID"
    , "EVENT_GROUP"
    -- , "EVENT_DATE"
    -- , "EVENT_TIME"
    , "EVENT_TIMESTAMP"
    , "EVENT_CATEGORY"
    , "EVENT_NAME"
    , "EVENT_DESCRIPTION"
    , "EVENT_LINKS"
  from "DEMO_DATA"
  where "EVENT_GROUP" = 3
)
file_format = (type = PARQUET compression = NONE)
header = TRUE
single = TRUE
;

-- View staged demo data
list @"STG__DEMO_DATA";

-- Output data if you'd like to view it directly
-- (Need to run in SnowSQL, VSCode extension or similar)
GET '@"STG__DEMO_DATA"/csv/' 'file://./data';
GET '@"STG__DEMO_DATA"/json/' 'file://./data';
GET '@"STG__DEMO_DATA"/parquet/' 'file://./data';
