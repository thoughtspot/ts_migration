--1) Stored Procedure for Migration Execution Header - 

CREATE OR REPLACE PROCEDURE Migration_Execution_Header(exec_id_input varchar)
  RETURNS STRING
  LANGUAGE SQL
AS
$$
BEGIN


Insert into migration_execution_header
select exec_id,
 exec_mode,exec_start_time,exec_end_time ,
 exec_status , twb_file_name , null as exec_error_details from RAW_DATA_DUMP
 where exec_mode is not null AND exec_id = :exec_id_input
 group by 1,2,3,4,5,6,7 ;


RETURN 'SUCCESS';

END;
$$;

