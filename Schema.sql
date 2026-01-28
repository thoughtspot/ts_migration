create or replace database TB_2_TS;
use database TB_2_TS;


DROP SCHEMA IF EXISTS TB_2_TS.TTTM_RAW;
CREATE SCHEMA TB_2_TS.TTTM_RAW;
USE SCHEMA TB_2_TS.TTTM_RAW
--1) Create table mIgration Execution Header : 
--1) Create table mIgration Execution Header : 


Create or replace table migration_execution_header (
exec_id varchar primary key,
exec_mode varchar ,
exec_start_time TIMESTAMP ,
exec_end_time TIMESTAMP,
exec_status varchar ,
twb_file_name varchar,
exec_error_details varchar
);

--2) TWB File
create or replace table twb_file (
twb_file_id varchar primary key ,
exec_id varchar foreign key references migration_execution_header(exec_id),
twb_file_name varchar
) ;

---3) Migration Execution Detail 
create or replace table migration_execution_detail(
exec_detail_id varchar primary key,
exec_id varchar foreign key references migration_execution_header(exec_id),
twb_file_id varchar foreign key references twb_file(twb_file_id),
exec_status varchar,
exec_error_details varchar
);

--4) Create table Datasource Header : 

create or replace table datasource_header(
 datasource_header_id varchar primary key,
 exec_id varchar foreign key references migration_execution_header(exec_id),
 twb_file_id varchar foreign key references twb_file(twb_file_id),
 datasource_name varchar,
 output_file_type varchar,
 output_file_name varchar
 );






---5)Create table Datasource Detail : 


create or replace table datasource_detail(
    datasource_detail_id varchar primary key,
    datasource_header_id varchar foreign key references datasource_header(datasource_header_id),
    property_type varchar,
    property_name varchar,
    property_value varchar, 
    conv_feasibility varchar ,
    ts_equivalent_hint varchar,
    migration_status varchar 
 ) ;

--- 6) Create table Worksheet Header : 


create or replace table worksheet_header (
 worksheet_header_id varchar primary key ,
 exec_id varchar foreign key references migration_execution_header(exec_id), 
 twb_file_id varchar foreign key references twb_file(twb_file_id), 
 worksheet_name varchar
) ;

---7) Create table worksheet_detail : 


create or replace table worksheet_detail(
    worksheet_detail_id varchar primary key,
    worksheet_header_id varchar foreign key references worksheet_header(worksheet_header_id),
    -- datasource_header_id varchar ,
    property_type varchar,
    property_name varchar,
    property_value varchar, 
    conv_feasibility varchar ,
    ts_equivalent_hint varchar
 ) ;

--8)-Dashboard Header 
create or replace table dashboard_header(
dashboard_header_id varchar primary key,
exec_id varchar foreign key references migration_execution_header(exec_id),
twb_file_id varchar foreign key references twb_file(twb_file_id), 
dashboard_name varchar
);
--9) Dashboard Details : 
create or replace table dashboard_detail(
    dashboard_detail_id varchar primary key,
    dashboard_header_id varchar foreign key references dashboard_header(dashboard_header_id),
    worksheet_header_id varchar foreign key references worksheet_header(worksheet_header_id),
    property_type varchar,
    property_name varchar,
    property_value varchar,
    conv_feasibility varchar ,
    ts_equivalent_hint string
);

--10) Create table header : 
create or replace table table_header(
    table_header_id varchar primary key,
    exec_id varchar foreign key references migration_execution_header(exec_id),
    table_name varchar,
    connection_tag varchar,
    type varchar
);

---11) Create table_detail : 
 create or replace table table_detail(
 table_detail_id varchar primary key , 
 table_header_id varchar foreign key references table_header(table_header_id),
 column_name varchar , 
 data_type varchar , 
 conv_feasibility varchar ,
 ts_data_type varchar ,
 migration_status varchar 
 );

---12)Create table table output file : 

Create or replace table table_output_file(
table_output_file_id varchar primary key ,
table_header_id varchar foreign key references table_header(table_header_id),
output_file_type varchar ,
output_file_name varchar ,
conversion_in_ts varchar
) ;


--13) Datasource Table Header Xref : 
Create or replace table datasource_table_xref(
datasource_table_xref_id varchar primary key , 
datasource_header_id varchar foreign key references datasource_header(datasource_header_id),
table_header_id varchar foreign key references table_header(table_header_id)
) ;


--14) Datasource column xref : 
Create or replace table datasource_column_xref(
datasource_column_xref_id varchar primary key , 
datasource_header_id varchar foreign key references datasource_header(datasource_header_id) ,
table_detail_id varchar foreign key references table_detail(table_detail_id)
) ;


---15) WORKSHEET DATASOURCE XREF TABLE : 
create or replace table worksheet_datasource_xref(
worksheet_datasource_xref_id varchar primary key, 
datasource_header_id varchar foreign key references datasource_header(datasource_header_id),
worksheet_header_id varchar foreign key references worksheet_header(worksheet_header_id),
twb_file_id varchar foreign key references twb_file(twb_file_id),
worksheet_name varchar
) ;

--  Worksheet Header 2
create or replace table worksheet_header2 (
 worksheet_header_id varchar primary key ,
 exec_id varchar , 
 twb_file_id varchar foreign key references twb_file(twb_file_id), 
 worksheet_name varchar
) ;
