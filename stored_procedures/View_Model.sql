--5) Create Stored Procedure for Views in the data model 

CREATE OR REPLACE PROCEDURE View_Model(exec_id_input VARCHAR)
  RETURNS STRING
  LANGUAGE SQL
AS
$$
BEGIN

-- WORKSHEET NOT USED IN DASHBOARD (VIEW) ==>

EXECUTE IMMEDIATE
'CREATE OR REPLACE VIEW STG_WORKSHEET_NOT_IN_DASHBOARD AS 
SELECT exec_id, twb_file_id, worksheet_header_id 
FROM TB_2_TS.TTTM_RAW.WORKSHEET_HEADER w
--WHERE w.exec_id = ''' || exec_id_input || '''
MINUS
SELECT exec_id, twb_file_id, worksheet_header_id 
FROM TB_2_TS.TTTM_RAW.DASHBOARD_DETAIL AS d
JOIN TB_2_TS.TTTM_RAW.DASHBOARD_HEADER AS d1 
ON d.DASHBOARD_HEADER_ID = d1.DASHBOARD_HEADER_ID 
--WHERE d1.exec_id = ''' || exec_id_input || ''';';


-- DATASOURCE NOT USED IN WORKSHEET (VIEW) ==>

EXECUTE IMMEDIATE
'CREATE OR REPLACE VIEW Stg_datasource_not_in_worksheet AS 
SELECT exec_id, twb_file_id, datasource_header_id 
FROM datasource_header dsh
--WHERE dsh.exec_id = ''' || exec_id_input || '''
MINUS
SELECT wsh.exec_id, wsh.twb_file_id, wd.datasource_header_id 
FROM worksheet_datasource_xref AS wd
LEFT JOIN worksheet_header wsh 
ON wd.worksheet_header_id = wsh.worksheet_header_id
--WHERE wsh.exec_id = ''' || exec_id_input || ''';';


-- TABLES CONNECTED WITH DATASOURCES LINKED WITH UNSUPPORTED DATA PLATFORM (VIEW) ==>

EXECUTE IMMEDIATE
'CREATE OR REPLACE VIEW stg_tables_connected_with_unsupported_data_platform AS
WITH base AS (
  SELECT xrf.datasource_header_id, th.exec_id, th.table_header_id, th.table_name, 
         REGEXP_SUBSTR(th.connection_tag, ''class="([^"]+)"'', 1, 1, ''e'', 1) AS class_value 
  FROM datasource_table_xref xrf
  LEFT JOIN table_header th 
  ON th.table_header_id = xrf.table_header_id
  --WHERE th.exec_id = ''' || exec_id_input || '''
)
SELECT dd.datasource_header_id, b.table_header_id, b.exec_id, b.table_name, b.class_value, 
       dd.property_name, dd.property_value, dd.conv_feasibility
FROM base b
LEFT JOIN datasource_detail dd 
ON dd.datasource_header_id = b.datasource_header_id 
WHERE dd.property_name = ''connector'' AND dd.conv_feasibility != ''Full''
GROUP BY dd.datasource_header_id, b.table_header_id, b.exec_id, b.table_name, b.class_value, 
         dd.property_name, dd.property_value, dd.conv_feasibility;';

RETURN 'SUCCESS';

END;
$$;











