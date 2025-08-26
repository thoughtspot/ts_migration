--4) Create Stored Procedure for DETAIL Table

CREATE OR REPLACE PROCEDURE Detail_table(exec_id_input varchar)
  RETURNS STRING
  LANGUAGE SQL
AS
$$
BEGIN


--MIGRATION EXECUTION DETAIL ==>

Insert into migration_execution_detail
with twb_table as (
select tf.exec_id, tf.twb_file_id , tf.twb_file_name from twb_file tf
where tf.exec_id = :exec_id_input
)

select md5(concat(
        coalesce(cast(t.exec_id as text),''),
        coalesce(cast(t.twb_file_id as text),''),
        coalesce(cast(dd.exec_error_details as text),'')
)) exec_detail_id,
t.exec_id, t.twb_file_id , dd.exec_status , dd.exec_error_details from RAW_DATA_DUMP dd
inner join twb_table t on t.twb_file_name = dd.twb_file_name 
where dd.exec_error_details is not null and dd.exec_id= :exec_id_input;


--DATASOURCE DETAIL ==>

Insert into datasource_detail
with dsh as (
select datasource_header_id, twb_file_id, datasource_name from datasource_header
where exec_id = :exec_id_input
),

twb_file_table as (
select tf.twb_file_id, tf.twb_file_name , dsh.datasource_header_id , dsh.datasource_name from twb_file tf
left join dsh on tf.twb_file_id = dsh.twb_file_id
where tf.exec_id = :exec_id_input
),

dsd as (
select 
dd.datasource_name, dd.property_type, dd.property_name, dd.property_value , dd.conversion_in_ts , dd.supported_in_ts , dd.twb_file_name, tt.datasource_header_id
--from RAW_DATA_DUMP dd inner join dsh on dd.datasource_name = dsh.datasource_name
from RAW_DATA_DUMP dd inner join twb_file_table tt on dd.datasource_name = tt.datasource_name and dd.twb_file_name = tt.twb_file_name
where (dd.object_type = 'datasource property' or (dd.object_type = 'column' and dd.property_type = 'calculated_column')) and 
dd.exec_id = :exec_id_input
)

select md5(concat(
        coalesce(cast(tt.datasource_header_id as text),''),
        coalesce(cast(dsd.property_value as text),'')
)) datasource_detail_id ,
tt.datasource_header_id ,dsd.property_type, dsd.property_name, dsd.property_value ,
-- ' ' as conv_feasibility, coalesce((dsd.conversion_in_ts ),' ') as ts_equivalent_hint,' ' as migration_status
coalesce((dsd.supported_in_ts),'  ') as conv_feasibility, coalesce((dsd.conversion_in_ts ),' ') as ts_equivalent_hint,' ' as migration_status
from dsd 
--inner join twb_file_table tt on dsd.datasource_name = tt.datasource_name  and dsd.twb_file_name = tt.twb_file_name;
inner join twb_file_table tt on dsd.datasource_header_id = tt.datasource_header_id order by dsd.property_type ;


--WORKSHEET DETAIL ==>

Insert into worksheet_detail
with base as (
select twb_file_name,object_type,datasource_name, worksheet_name , property_type , property_name , property_value , conversion_in_ts, supported_in_ts
from RAW_DATA_DUMP
where worksheet_name is not null and exec_id= :exec_id_input),

wsh as (
select * from worksheet_header
where exec_id = :exec_id_input
),

dsn as (
select wsh.worksheet_header_id, wsh.worksheet_name, b.datasource_name 
from base b inner join wsh on b.worksheet_name = wsh.worksheet_name
),

joined_table as (
select wsh.worksheet_header_id , b.property_type, b.property_name , b.property_value , b.conversion_in_ts, b.supported_in_ts
from wsh 
inner join base b on b.worksheet_name = wsh.worksheet_name 
where b.object_type in ('chart', 'chart property') and b.property_value is not null
)

select md5(concat(
        coalesce(cast(jt.worksheet_header_id as text),''),
        coalesce(cast(jt.property_value as text),'')
)) worksheet_detail_id,
jt.worksheet_header_id , jt.property_type , jt.property_name , jt.property_value,
--' ' as conv_feasibility_status_id , '  ' as ts_recommended_value from joined_table jt;
coalesce((jt.supported_in_ts),' ') as conv_feasibility_status_id , coalesce((jt.conversion_in_ts),' ') as ts_recommended_value from joined_table jt;


--DASHBOARD DETAIL ==>

Insert into dashboard_detail
with dashboard_header_table as (
select dsh.dashboard_header_id, dsh.dashboard_name from dashboard_header dsh
where dsh.exec_id = :exec_id_input
),

dump_table as (
select dd.object_type,dd.worksheet_name, dd.dashboard_name, dd.property_type, dd.property_name, dd.property_value, dd.conversion_in_ts , dd.supported_in_ts from RAW_DATA_DUMP dd
where dd.exec_id= :exec_id_input
),

worksheet_header_table as (
select wsh.worksheet_header_id, wsh.worksheet_name from worksheet_header wsh
where wsh.exec_id = :exec_id_input
),

joined_1 as (
select dsht.dashboard_header_id,dt.worksheet_name ,dsht.dashboard_name, dt.property_type, dt.property_name, dt.property_value,dt.conversion_in_ts , dt.supported_in_ts,
coalesce((dt.supported_in_ts),' ') as conv_feasibility , coalesce((dt.conversion_in_ts),' ') as ts_equivalent_hint
from dashboard_header_table dsht
inner join dump_table dt on dt.dashboard_name = dsht.dashboard_name
where dt.object_type in ('dashboard','dashboard property') and dt.property_value is not null
)

select md5(concat(
        coalesce(cast(j.dashboard_header_id as text),''),
        coalesce(cast(wsht.worksheet_header_id as text),''),
        coalesce(cast(j.property_value as text),'')
        )) as dashboard_detail_id,
j.dashboard_header_id,wsht.worksheet_header_id,j.property_type, j.property_name, j.property_value,
j.conv_feasibility , j.ts_equivalent_hint
from joined_1 j 
left join worksheet_header_table wsht on wsht.worksheet_name = j.worksheet_name ;

--TABLE DETAIL ==> 
Insert into table_detail
WITH tableheader_detail AS (
SELECT th.table_header_id, th.table_name,
        CASE 
            WHEN th.table_name LIKE '[%].[%].[%]' THEN 
                substr(split_part(th.table_name, '.', 3), 2, length(split_part(th.table_name, '.', 3))-2)
            WHEN th.table_name LIKE '[%' THEN 
                substr(th.table_name, 2, length(th.table_name)-2)
            ELSE 
                th.table_name
        END AS table_name_splitted
    FROM 
        table_header th
        where type='table'
),

dump_table as (
select dd.table_name ,dd.local_column_name , dd.object_type , dd.property_type , dd.property_value , dd.conversion_in_ts , dd.supported_in_ts from RAW_DATA_DUMP dd
where dd.object_type = 'column' and dd.property_type='metadata-record'
)

select md5(concat(
        coalesce(cast(td.table_header_id as text),''),
        coalesce(cast(dt.local_column_name as text),'')
        )) table_detail_id ,
td.table_header_id ,dt.local_column_name, dt.property_value as data_type,
--' ' as conv_feasibility_status_id,' ' as ts_recommended_data_type,'' as migr_status_id
dt.supported_in_ts as conv_feasibility_status_id,dt.conversion_in_ts as ts_recommended_data_type,'' as migr_status_id
from tableheader_detail td
inner join dump_table dt on dt.table_name = td.table_name_splitted 
group by 1,2,3,4,5,6 ;


--DATASOURCE TABLE HEADER XREF ==>

Insert into datasource_table_xref
with datasource_header_table as (
select dsh.datasource_header_id, dsh.datasource_name , replace(replace(dd.table_name, '#', '.'), '$', '') as table_name from datasource_header dsh
left join RAW_DATA_DUMP dd on dsh.datasource_name = dd.datasource_name
--where dd.object_type = 'table' and dd.exec_id = :exec_id_input and dsh.exec_id = :exec_id_input)
where dd.object_type in ('table','custom sql query') and dd.exec_id = :exec_id_input and dsh.exec_id = :exec_id_input)

select md5(concat(
        coalesce(cast(dst.datasource_header_id as text),''),
        coalesce(cast(th.table_header_id as text),'')
)) datasource_table_xref_id ,
dst.datasource_header_id , th.table_header_id from datasource_header_table dst
left join table_header th on dst.table_name = th.table_name 
where th.exec_id = :exec_id_input 
group by 1, 2, 3;


--DATASOURCE COLUMN XREF ==>

Insert into datasource_column_xref
with base as (
select td.table_detail_id , td.table_header_id from table_detail td
),

header_table as (
select b.table_detail_id ,th.table_header_id , th.table_name  from base b 
left join table_header th on th.table_header_id = b.table_header_id
where th.exec_id = :exec_id_input
) , 

dump_table as (
select ht.table_detail_id , ht.table_header_id , ht.table_name , dd.datasource_name from header_table ht 
--left join RAW_DATA_DUMP dd on  ht.table_name = dd.table_name
left join RAW_DATA_DUMP dd on  ht.table_name = replace(replace(dd.table_name, '#', '.'), '$', '')
where dd.exec_id = :exec_id_input
)


select md5(concat(
        coalesce(cast(dsh.datasource_header_id as text),''),
        coalesce(cast(dt.table_detail_id as text),'')
)) datasource_column_xref_id ,
 dsh.datasource_header_id ,dt.table_detail_id  from dump_table dt 
left join datasource_header dsh on dt.datasource_name = dsh.datasource_name 
where dsh.exec_id = :exec_id_input 
group by 1,2,3;

--WORKSHEET DATASOURCE XREF TABLE ==>

Insert into worksheet_datasource_xref
with wsd as (
select * from worksheet_detail
where property_type = 'datasource'
),

worksheet_header_table as (
select md5(concat(
        coalesce(cast(wsh.exec_id as text),''),
        coalesce(cast(wsh.twb_file_id as text),''),
        coalesce(cast(wsd.property_value as text),'')
)) datasource_header_id,
wsh.worksheet_header_id, wsh.exec_id, wsh.twb_file_id , wsh.worksheet_name , wsd.property_value  from wsd 
left join worksheet_header wsh on wsh.worksheet_header_id = wsd.worksheet_header_id 
where wsh.exec_id = :exec_id_input
)

select md5(concat(
        coalesce(cast(dsh.datasource_header_id as text),''),
        coalesce(cast(wht.worksheet_header_id as text),'')
)) worksheet_datasource_xref_id,
dsh.datasource_header_id,wht.worksheet_header_id, wht.twb_file_id, wht.worksheet_name from worksheet_header_table wht 
left join datasource_header dsh on dsh.datasource_header_id = wht.datasource_header_id 
where dsh.datasource_header_id is not null and dsh.exec_id = :exec_id_input
order by worksheet_name;


--TABLE OUTPUT FILE ==> 

Insert into table_output_file
with dump_table as (
select replace(replace(dd.table_name, '#', '.'), '$', '') as table_name, dd.conversion_in_ts , dd.object_type,dd.output_file_type,dd.output_file_name from RAW_DATA_DUMP dd
where dd.exec_id = :exec_id_input and  (dd.object_type='table' or (dd.object_type='custom sql query' and property_type='text'))
),

table_output_file as (
select th.table_header_id , th.table_name from table_header th
where th.exec_id = :exec_id_input
)

select md5(concat(
    coalesce(cast(tof.table_header_id as text),''),
    coalesce(cast(dt.output_file_type as text),''),
    coalesce(cast(dt.output_file_name as text),'')
)) as table_output_file_id,
tof.table_header_id,dt.output_file_type,dt.output_file_name,dt.conversion_in_ts from dump_table dt 
left join table_output_file tof on dt.table_name = tof.table_name ;


  RETURN 'SUCCESS';

END;
$$;

