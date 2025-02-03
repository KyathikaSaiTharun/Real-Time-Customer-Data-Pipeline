create or replace stream real_time_data_stream on table customers ;        

create or replace procedure stored_procedure()
returns string not null
language javascript
as
    $$
      var merge_query = `
                 merge into customers c 
                            using customers_raw cr
                            on c.customer_id = cr.customer_id
                            when matched and c.customer_id <> cr.customer_id or
                                             c.first_name <> cr.first_name or
                                             c.last_name <> cr.last_name or
                                             c.email <> cr.email or
                                             c.street <> cr.street or
                                             c.city <> cr.city or
                                             c.state <> cr.state or
                                             c.country <> cr.country
                             then update set 
                                            c.customer_id = cr.customer_id,
                                             c.first_name = cr.first_name,
                                             c.last_name = cr.last_name,
                                             c.email = cr.email,
                                             c.street = cr.street,
                                             c.city = cr.city,
                                             c.state = cr.state,
                                             c.country = cr.country,
                                             c.update_time = current_timestamp()
                             when not matched then insert                                           
         (c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country)
                                    values                 (cr.customer_id,cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);
      `
      var trunc_raw = 'truncate table customers_raw;'
      var sql = snowflake.createStatement({sqlText : merge_query});
      var sql1 = snowflake.createStatement({sqlText : trunc_raw});
      var res_merge = sql.execute();
      var res_trunc = sql1.execute();
    return merge_query+'\n'+trunc_raw;
    $$;
 call stored_procedure()   


 use role securityadmin;

 create or replace role taskadmin;

 use role accountadmin;

 grant execute task on account to  role taskadmin; 

 use role securityadmin;
 grant role taskadmin to role sysadmin;


 create or replace task trigger_merge_function warehouse = COMPUTE_WH schedule = '1 minute'
 error_on_nondeterministic_merge = false
 as
 call stored_procedure();

show tasks;

alter task trigger_merge_function resume;
alter task trigger_merge_function suspend;

show streams;
select * from REAL_TIME_DATA_STREAM;



create or replace view customer_history_view as

select customer_id,first_name,last_name,email,street,city,state,country,start_time,end_time,is_latest,'I' as dml_type from
(select customer_id,first_name,last_name,email,street,city,state,country,
update_time as start_time,
lag(update_time) over(partition by customer_id order by update_time desc) as end_time_temp,
case when end_time_temp is null then '9999-12-31'::timestamp_ntz else end_time_temp end as end_time,
case when end_time_temp is null then true else false end as is_latest from
(select customer_id,first_name,last_name,email,street,city,state,country,update_time
from REAL_TIME_DATA_STREAM where metadata$action = 'INSERT' and metadata$isupdate = 'FALSE')
)
union

select customer_id,first_name,last_name,email,street,city,state,country,start_time,end_time,is_latest,dml_type from
(select customer_id,first_name,last_name,email,street,city,state,country,
update_time as start_time,
lag(update_time) over(partition by customer_id order by update_time desc) as end_time_temp,
case when end_time_temp is null then '9999-12-31'::timestamp_ntz else end_time_temp end as end_time,
case when end_time_temp is null then true else false end as is_latest,dml_type from
(select customer_id,first_name,last_name,email,street,city,state,country,update_time,'I' as dml_type
from REAL_TIME_DATA_STREAM where metadata$action = 'INSERT' and metadata$isupdate = 'TRUE'
union
select customer_id,null,null,null,null,null,null,null,start_time,'D' as dml_type
from customers_history where (select distinct customer_id from REAL_TIME_DATA_STREAM where metadata$action = 'DELETE' and metadata$isupdate = 'TRUE') and is_latest = true
))

union
select ch.customer_id,null,null,null,null,null,null,null,ch.start_time,current_timestamp()::timestamp_ntz,null,'D'
from customers_history ch
inner join REAL_TIME_DATA_STREAM rtd on rtd.customer_id = ch.customer_id
where rtd.metadata$action = 'DELETE'
and   rtd.metadata$isupdate = 'FALSE'
and   ch.is_latest = TRUE;



select * from customer_history_view;


create or replace task customer_history_task warehouse = compute_wh schedule = '1 minute'
error_on_nondeterministic_merge = false
as
merge into customers_history ch
using customer_history_view chv
on ch.customer_id = chv.customer_id and ch.start_time = chv.start_time
when matched and chv.dml_type = 'U' then update set ch.end_time = chv.end_time and ch.is_latest = false
when matched and chv.dml_type = 'D' then update set ch.end_time = chv.end_time and ch.is_latest = false
when not matched and chv.dml_type = 'I' then insert (customer_id,first_name,last_name,email,street,city,state,country,start_time,end_time,is_latest) 
values (chv.customer_id,chv.first_name,chv.last_name,chv.email,chv.street,chv.city,chv.state,chv.country,chv.start_time,chv.end_time,chv.is_latest);
        
show tasks;
