-- Answers to the questions in snowflake SQL queries
-- How many number of operations serviced by day wise?

select date,DAYNAME(date) as weekday,count(encounter_id) as OPS_COUNT
from OR_UTIL_1
group by date 
order by date;

-- In which weekdays , the number of operations is highest ?

select top 1 DAYNAME(date) as weekday,count(encounter_id) as OPS_COUNT
from OR_UTIL_1
group by weekday
order by OPS_COUNT desc;


-- How many unique operations serviced ? 

select count(distinct cpt_code) as unique_ops_count
from OR_UTIL_1

-- What's the common procedures serviced?

select cpt_code,
CPT_DESC as procedure_name,
service as department,
count(cpt_code) as operation_count
from OR_UTIL_1
group by cpt_code,CPT_DESC,service
qualify rank() over (order by operation_count desc) = 1

-- How many number of procedures delayed from booked time ?

select count(encounter_id) as opn_delay,count(distinct cpt_code) as delayed_procedure from OR_UTIL_1
where actual_time_taken > booked_time  -- 1675/2173

-- What procedure most delayed from booked time ?

select cpt_code,
cpt_desc,
round(sum(actual_time_taken/60)/60,2) as actual_time_taken_sum,
round(sum(booked_time/60)/60,2) as booked_time_sum,
round(sum(actual_time_taken/60-booked_time/60)/60,2) as delayed_time_in_hour,
dense_rank() over (order by delayed_time_in_hour desc) as time_rank
from OR_UTIL_1
group by cpt_code,cpt_desc
qualify time_rank < 2;

-- What is the average time to take the patient from scheduled time to wheels in time ?


select round(avg(OR_IN),2) as average_time_in_minutes
from OR_UTIL_1 

-- Which procedure taken longest and short time  from start and end time ?

select distinct cpt_desc as procedure_name
from OR_UTIL_1
where START_END in (select max(START_END) from OR_UTIL_1) 


select distinct cpt_desc as procedure_name
from OR_UTIL_1
qualify rank() over (order by start_end asc) = 1


-- Which type of service takes long time to complete the operation?

select service,sum(actual_time_taken/60)/60 as time_taken
from OR_UTIL_1
group by service
qualify rank() over (order by time_taken desc) = 1

-- Compare the total time taken with the previous years for the procedure 'Arthroplasty, knee, hinge prothesis' ?

select CPT_DESC,
year(to_date(DATE,'YYYY-MM-DD')) as YEAR_FROM_DATE,
month(to_date(DATE,'YYYY-MM-DD')) as MONTH_FROM_DATE,
sum(ACTUAL_TIME_TAKEN) as TIME_TAKEN,
sum(ACTUAL_TIME_TAKEN)-LAG(sum(ACTUAL_TIME_TAKEN),1,0) OVER (PARTITION by CPT_DESC order by month(to_date(DATE,'YYYY-MM-DD'))) as diff_to_prev
from OR_UTIL_1
where CPT_DESC = 'Arthroplasty, knee, hinge prothesis'
group by CPT_DESC , year(to_date(DATE,'YYYY-MM-DD')), month(to_date(DATE,'YYYY-MM-DD'))
order by CPT_DESC, month(to_date(DATE,'YYYY-MM-DD'));