create or replace database stream_db;
create or replace schema stream_sc;

create or replace table customers(
customer_id int,
first_name varchar(255),
last_name varchar(255),
email varchar(100),
street varchar,
city varchar,
state varchar,
country varchar,
update_time timestamp_ntz default current_timestamp()
);


create or replace table customers_history(
customer_id int,
first_name varchar(255),
last_name varchar(255),
email varchar(100),
street varchar,
city varchar,
state varchar,
country varchar,
start_time timestamp_ntz default current_timestamp(),
end_time timestamp_ntz default current_timestamp(),
is_latest boolean
);

create or replace table customers_raw(
customer_id int,
first_name varchar(255),
last_name varchar(255),
email varchar(100),
street varchar,
city varchar,
state varchar,
country varchar);




create or replace storage integration s3_connect
type = external_stage
storage_provider = S3
enabled = true
storage_aws_role_arn = 'arn:aws:iam::600627350141:role/snowflake-s3-real-time-data'
storage_allowed_locations = ('s3://realtime-streaming-pipeline/snowflake-files/');

create or replace file format csv_format
type = csv
skip_header = 1
field_delimiter = ','
null_if = ('NULL','null')
empty_field_as_null = true;

create or replace stage stage_real_time
url = 's3://realtime-streaming-pipeline/snowflake-files/'
storage_integration = s3_connect
file_format = csv_format


create or replace pipe snowpipe_real_time
auto_ingest = true
as
copy into customers_raw
from @stage_real_time;

show pipes
select SYSTEM$PIPE_STATUS('snowpipe_real_time')
