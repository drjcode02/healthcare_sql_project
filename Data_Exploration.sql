/* structure of snowflake - Warehouse -> database -> schema -> stages or tables */

-- Query 1 create , alter and desc and drop the warehouses 

Create or replace warehouse PROJECT1_WH WITH
    Warehouse_type = 'STANDARD' --COMMENT = 'It handles Memory part of the warehouse - Standard or Snowpark-Optimized'
    Warehouse_size = 'X-SMALL' --COMMENT = 'Cluster size to do data operations,XS is default, check site for other sizes'
    MAX_CLUSTER_COUNT = 1 --COMMENT = 'Def : 1 cluster count is the number of nodes to do operation - 1 to 10 values'
    MIN_CLUSTER_COUNT = 1 --COMMENT = 'If both parameters are equal, the warehouse runs in Maximized mode,If MIN_CLUSTER_COUNT is less than MAX_CLUSTER_COUNT, the warehouse runs in Auto-scale mode'
    SCALING_POLICY = 'STANDARD' --COMMENT = 'STANDARD OR ECONOMY ( used for multi cluster warehouse to set the policy for start and shutdown the function'
    AUTO_SUSPEND = 300 --COMMENT = 'Default 600(10 minutes , the warehouse will go inactive. set 0 or NULL to never suspend'
    AUTO_RESUME = TRUE --COMMENT = 'TRUE OR FALSE , automatically resume a warehouse when a SQL statement , def: TRUE'
    INITIALLY_SUSPENDED = FALSE --COMMENT = 'def: TRUE - Specifies whether the warehouse is created initially in the ‘Suspended’ state'
    ENABLE_QUERY_ACCELERATION = FALSE --COMMENT = 'SET TRUE to control usage of compute resources and use below parameter to set the value, improve performance of the warehouse by running the query in a systematic way'
    QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8 --COMMENT = 'def- 8 , 0 to 100 control usage of compute resources , available after Enterprise edition'
    COMMENT = 'Only the SYSADMIN role, or a higher role, has this privilege by default';

/* how can check the existing warehouse or update the parameters of it*/

Describe warehouse COMPUTE_WH; /* shows the details of warehouse like when it created and kind */
show warehouses; /*to check the properties of the warehouse and its status */
Alter warehouse set COMMENT = 'Warehouse created for practice,Only the SYSADMIN role, or a higher role, has this privilege to create WH'; /*update the warehouse comment parameter*/

/* Tips on dropping  the warehouse 

First suspend the warehouse to stop executing queries or any operations working on that warehouse
Drop the warehouse

SQL:

Alter warehouse PROJECT1_WH suspend/resume;
Drop warehouse PROJECT1_WH;


Rename the warehouse - alter warehouse if exists PROJECT1_WH rename to PROJECT2_WH;

*/

-- Query 2 

--create , alter and desc and drop the databases - permanent and transient database


Create or replace  database healthcare 
    with data_retention_time_in_days=30
    MAX_DATA_EXTENSION_TIME_IN_DAYS=3;
    --TAG (datalevel ='healthcare data'); 
    
--   data_retention_time_in_days sets  data retention period of 10 days.

Alter database healthcare set data_retention_time_in_days = 30; -- alter the parameter values
Alter database healthcare set COMMENT = 'databases reserved for storing the healthcare related datasets'; -- add comments 
Alter database healthcare unset data_retention_time_in_days; -- unset is to restore the default values for parameters
describe database healthcare; -- describe about database healthcare
show databases; -- list of tables with parameters.


-- Drop database healthcare; -- depends upon the retention period, the data base will go to time travel , then it goes to fail safe for 7 days
SHOW DATABASES HISTORY LIKE 'healthcare';


--Drop database if exists healthcare cascade;

-- Query 3 

-- Work with schema

Create or replace schema HIHS
with data_retention_time_in_days= 30
MAX_DATA_EXTENSION_TIME_IN_DAYS = 3
comment = 'Hospital Stay data';

alter schema HIHS set comment = 'Hospital Stay data analysis'

show schemas history like '%hihs%';

show schemas;

-- Query 4

-- create stage to locate the file folder 

create or replace stage hospital_stay_folder;

show stages;

list @HEALTHCARE.HIHS.HOSPITAL_STAY_FOLDER;

--use snowsql to load local files from the system to the stage
/*put file://C:\Users\User\Documents\Kaggle_datasets\OR_Utilization\2022_Q1_OR_Utilization.csv @HEALTHCARE.HIHS.HOSPITAL_STAY_FOLDER;*/

-- create file format for the file in the folder 

CREATE OR REPLACE file format my_file_format with skip_header = 1 type=csv field_delimiter=',';

desc file format my_file_format;

--Create destination table 

create or replace table OR_UTIL 
(
--index number autoincrement start 1 increment 1,
index int,
Encounter_ID int,
Date varchar(30),
OR_SUITE int,
Service varchar(50),
CPT_Code int,
CPT_Desc varchar(100),
Booked_Time int,
OR_schedule varchar(24),
Wheels_in varchar(24),
Start_time varchar(24),
End_time varchar(24),
Wheels_out varchar(24)
);

--load the files

copy into OR_UTIL
from
(select 
h.$1,
h.$2,
h.$3,
h.$4,
h.$5,
h.$6,
h.$7,
h.$8,
h.$9,
h.$10,
h.$11,
h.$12,
h.$13

from
@HEALTHCARE.HIHS.HOSPITAL_STAY_FOLDER as h)
file_format=(FORMAT_NAME=HEALTHCARE.HIHS.MY_FILE_FORMAT FIELD_OPTIONALLY_ENCLOSED_BY = '"')
files = ('2022_Q1_OR_Utilization.csv.gz')
ON_ERROR= 'SKIP_FILE';

create or replace table OR_UTIL_1 
(
--index number autoincrement start 1 increment 1,
index int,
Encounter_ID int,
Date varchar(30),
OR_SUITE int,
Service varchar(50),
CPT_Code int,
CPT_Desc varchar(100),
Booked_Time int,
OR_schedule timestamp,
Wheels_in timestamp,
Start_time timestamp,
End_time timestamp,
Wheels_out timestamp
);

-- insert the data for the date format without AM or PM

insert into OR_UTIL_1
select 
index,
Encounter_ID,
Date,
OR_SUITE,
Service,
CPT_Code,
CPT_Desc,
Booked_Time,
to_timestamp(OR_schedule,'DD-MM-YYYY HH24:MI'),
to_timestamp(Wheels_in,'DD-MM-YYYY HH24:MI') ,
to_timestamp(Start_time,'DD-MM-YYYY HH24:MI'),
to_timestamp(End_time,'DD-MM-YYYY HH24:MI'),
to_timestamp(Wheels_out,'DD-MM-YYYY HH24:MI') 
from OR_UTIL where wheels_in not like '%M%';

--insert the data with date format AM or PM

insert into OR_UTIL_1
select 
index,
Encounter_ID,
Date,
OR_SUITE,
Service,
CPT_Code,
CPT_Desc,
Booked_Time,
to_timestamp(concat((to_varchar(to_date(to_varchar(to_date(substr(OR_schedule,1,8),'MM/DD/YY'),'YYYYMMDD'),'YYYYMMDD'))),' '
,to_time(substr(OR_schedule,9,len(OR_schedule))))) as OR_schedule,
to_timestamp(concat((to_varchar(to_date(to_varchar(to_date(substr(Wheels_in,1,8),'MM/DD/YY'),'YYYYMMDD'),'YYYYMMDD'))),' '
,to_time(substr(Wheels_in,9,len(OR_schedule))))) as Wheels_in,
to_timestamp(concat((to_varchar(to_date(to_varchar(to_date(substr(Start_time,1,8),'MM/DD/YY'),'YYYYMMDD'),'YYYYMMDD'))),' '
,to_time(substr(Start_time,9,len(OR_schedule)))))  as Start_time,
to_timestamp(concat((to_varchar(to_date(to_varchar(to_date(substr(End_time,1,8),'MM/DD/YY'),'YYYYMMDD'),'YYYYMMDD'))),' '
,to_time(substr(End_time,9,len(OR_schedule)))))  as End_time,
to_timestamp(concat((to_varchar(to_date(to_varchar(to_date(substr(Wheels_out,1,8),'MM/DD/YY'),'YYYYMMDD'),'YYYYMMDD'))),' '
,to_time(substr(Wheels_out,9,len(OR_schedule)))))   as Wheels_out
from OR_UTIL where wheels_in like  '%M%';

-- two different date fields in DATE column , transforming into single date format

select to_date(DATE,'DD-MM-YYYY') from OR_UTIL_1 where len(DATE) = 10 ;
update OR_UTIL_1 set DATE = to_date(DATE,'DD-MM-YYYY') where len(DATE) = 10;

select to_date(DATE,'MM/DD/YY') from OR_UTIL_1 where len(DATE) = 8 ;
update OR_UTIL_1 set DATE = to_date(DATE,'MM/DD/YY') where len(DATE) = 8;

-- select all data from OR_UTIL_1 order by index column ascending , verify the date have transformed in the right format from varchar

select * from OR_UTIL_1 order by 1;


with cte as (
  SELECT
      COUNT(*) AS total_rows
      ,total_rows - COUNT(Encounter_ID) as encounter_null
      ,total_rows - COUNT(Date) as date_null
      ,total_rows - COUNT(OR_SUITE) as OR_SUITE_null
      ,total_rows - COUNT(Service) as Service_null
      ,total_rows - COUNT(CPT_Code) as CPT_Code_null
      ,total_rows - COUNT(CPT_Desc) as CPT_Desc_null
      ,total_rows - COUNT(Booked_Time) as Booked_Time_null
      ,total_rows - COUNT(OR_schedule) as OR_schedule_null
      ,total_rows - COUNT(Wheels_in) as Wheels_in_null
      ,total_rows - COUNT(Start_time) as Start_time_null
      ,total_rows - COUNT(End_time) as End_time_null
      ,total_rows - COUNT(Wheels_out) as Wheels_out_null
  FROM OR_UTIL_1
  )

-- count of nulls in row wise to column wise using unpivot using cte table

  select column_name,NULLS_COLUMN_COUNT,SUM(NULLS_COLUMN_COUNT) over() as NULLS_TOTAL_COUNT
  from cte
  unpivot (NULLS_COLUMN_COUNT for column_name in (encounter_null,date_null,OR_SUITE_null,Service_null,CPT_Code_null,CPT_Desc_null
  ,Booked_Time_null,OR_schedule_null,Wheels_in_null,Start_time_null,End_time_null,Wheels_out_null))
  order by column_name;

-- check the distinct data for classification columns 

select distinct service,count(*) from OR_UTIL_1
group by service


-- Adding extra column to get the actual time = difference of wheels_out and wheels_in
  
alter table OR_UTIL_1 add actual_time_taken NUMBER(38,0);

alter table OR_UTIL_1 add OR_IN NUMBER(38,0);

alter table OR_UTIL_1 add IN_START NUMBER(38,0);

alter table OR_UTIL_1 add START_END NUMBER(38,0);

alter table OR_UTIL_1 add END_OUT NUMBER(38,0);

update OR_UTIL_1 set OR_IN= datediff(min,OR_schedule,Wheels_in);

update OR_UTIL_1 set IN_START= datediff(min,Wheels_in,Start_time);

update OR_UTIL_1 set START_END= datediff(min,Start_time,End_time);

update OR_UTIL_1 set END_OUT= datediff(min,End_time,Wheels_out);

update OR_UTIL_1  set actual_time_taken = OR_IN+IN_START+START_END+END_OUT;