
--2) Create Stored Procedure for TWB File

CREATE OR REPLACE PROCEDURE twb_file(exec_id_input varchar)
  RETURNS STRING
  LANGUAGE SQL
AS
$$
BEGIN

Insert into twb_file
with migration_table as (
select me.exec_id, me.twb_file_name from migration_execution_header me
where me.exec_id = :exec_id_input
),

dump_table as (
select dd.twb_file_name from RAW_DATA_DUMP dd
where dd.exec_id = :exec_id_input
)

select md5(concat(
        coalesce(cast(mt.exec_id as text),''),
        coalesce(cast(dt.twb_file_name as text),'')
        )) twb_file_id,
mt.exec_id , dt.twb_file_name from migration_table mt 
inner join dump_table dt on dt.twb_file_name = mt.twb_file_name
group by 1,2,3 ;


 RETURN 'SUCCESS';

END;
$$;