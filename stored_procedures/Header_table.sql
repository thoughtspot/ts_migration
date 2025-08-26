--3) Create Stored Procedure for Header Table

CREATE OR REPLACE PROCEDURE Header_table(exec_id_input varchar)
  RETURNS STRING
  LANGUAGE SQL
AS
$$
BEGIN


--DATASOURCE HEADER ==>

Insert into datasource_header
with twb_table_file as (
select twb.exec_id, twb.twb_file_name, twb.twb_file_id from twb_file twb
where  twb.exec_id = :exec_id_input
),

dump_table as (
select dd.twb_file_name,dd.object_type ,dd.datasource_name ,dd.property_name, dd.property_value,dd.output_file_type,dd.output_file_name from RAW_DATA_DUMP dd
where dd.exec_id = :exec_id_input
)

select  md5(concat(
        coalesce(cast(tf.exec_id as text),''),
        coalesce(cast(tf.twb_file_id as text),''),
        coalesce(cast(dt.datasource_name as text),'')
        )) datasource_header_id,
tf.exec_id,tf.twb_file_id,dt.datasource_name,
dt.output_file_type,
dt.output_file_name
from twb_table_file tf
inner join dump_table dt on dt.twb_file_name = tf.twb_file_name
where dt.object_type = 'datasource property' and dt.property_name = 'connection' 
group by  1,2, 3,4 ,5,6;


--WORKSHEET HEADER ==>

Insert into worksheet_header
with table_file as (
select twb.exec_id , twb.twb_file_id , twb.twb_file_name from twb_file twb
where twb.exec_id = :exec_id_input
), 

dump_table as (
select dd.twb_file_name ,dd.worksheet_name , dd.object_type from RAW_DATA_DUMP dd
where dd.exec_id = :exec_id_input
)

select md5(concat(
        coalesce(cast(tf.exec_id as text),'' ),
        coalesce(cast(tf.twb_file_id as text),''),
        coalesce(cast(dt.worksheet_name as text),'')
        )) worksheet_header_id,
tf.exec_id, tf.twb_file_id , dt.worksheet_name from table_file tf 
inner join dump_table dt on dt.twb_file_name = tf.twb_file_name
where dt.object_type = 'chart' 
group by 1, 2, 3,4 ;


--DASHBOARD HEADER ==>

Insert into dashboard_header
with table_file as (
select twb.exec_id , twb.twb_file_id , twb.twb_file_name from twb_file twb
where twb.exec_id = :exec_id_input
), 

dump_table as (
select dd.object_type , dd.dashboard_name , dd.twb_file_name from RAW_DATA_DUMP dd
where dd.exec_id = :exec_id_input
)

select md5(concat(
        coalesce(cast(tf.exec_id as text),''),
        coalesce(cast(tf.twb_file_id as text),''),
        coalesce(cast(dt.dashboard_name as text),'')
        )) dashboard_header_id ,
tf.exec_id, tf.twb_file_id , dt.dashboard_name from table_file tf
inner join dump_table dt on dt.twb_file_name = tf.twb_file_name
where dt.object_type = 'dashboard' and dt.dashboard_name is not null
group by 1, 2, 3 , 4 ;


--TABLE HEADER ==>

Insert into table_header
with table_file as (
select twb.exec_id , twb.twb_file_name from twb_file twb
where twb.exec_id = :exec_id_input
), 

dump_table as (
select dd.twb_file_name , dd.table_name , dd.object_type , dd.property_value from RAW_DATA_DUMP dd
where dd.exec_id= :exec_id_input
)

select md5(concat(
        coalesce(cast(tf.exec_id as text), ''),
        coalesce(cast(replace(replace(dt.table_name, '#', '.'), '$', '') as text),''),
        coalesce(cast(dt.property_value as text),'')
        )) table_header_id,
tf.exec_id , replace(replace(dt.table_name, '#', '.'), '$', '') as table_name, dt.property_value as connection_tag ,'table' as type from table_file tf
inner join dump_table dt on dt.twb_file_name = tf.twb_file_name
where dt.object_type = 'table' and dt.table_name is not null
group by 1, 2, 3, 4 ,5

union

select md5(concat(
        coalesce(cast(dd.exec_id as text),''),
        coalesce(cast(dd.table_name as text),''),
        coalesce(cast(dd.property_value as text),'')
)) table_header_id,
dd.exec_id, dd.table_name , dd.property_value as connection_tag , 'custom sql query' as type from RAW_DATA_DUMP dd
where object_type = 'custom sql query' and property_type ='connection tag' and exec_id= :exec_id_input
group by 1,2,3,4,5 ;




 RETURN 'SUCCESS';

END;
$$;

