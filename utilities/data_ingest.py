from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from dotenv import load_dotenv
from pathlib import Path
import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col


class DataIngress:

    def __init__(self):
        dotenv_path = Path('twb_parser/.env')
        load_dotenv(dotenv_path=dotenv_path)
        self.user = os.getenv('SNOWFLAKE_USER')
        self.password = os.getenv('SNOWFLAKE_PASSWORD')
        self.account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        self.database = os.getenv('SNOWFLAKE_DATABASE')
        self.schema = os.getenv('SNOWFLAKE_SCHEMA')
        self.role = os.getenv('SNOWFLAKE_ROLE')


    def write_dump_data(self, df):

        # Create a connection engine
        engine = create_engine(URL(
            user= self.user,
            password= self.password,
            account= self.account,
            warehouse= self.warehouse,
            database= self.database,
            schema= self.schema,
            role= self.role
        ))
        df.columns = df.columns.str.replace(" ", "_")
        df = df.rename(columns={"conversion_in_TS": 'conversion_in_ts', "supported_in_TS": 'supported_in_ts', "supported_in_Migrator": 'supported_in_migrator'})
        df = df.astype(str)

        n=600
        list_df = [df[i:i+n] for i in range(0,len(df),n)]
        for df in list_df:
            try:            
                # Write the DataFrame to a Snowflake table
                df.to_sql('raw_data_dump', con=engine, if_exists='append', index=False)

                print('Data Ingested Successfully for ' + str(df.index[-1]) + ' records')
            except Exception as e:
                print('Data Ingestion failed for ' + str(df.index[-1]) + ' records for the exec_id: ' + str(df['exec_id'].iloc[0]))
                print(e)


        # Close the connection
        engine.dispose()

    def create_session(self):
        try:
            # Call the stored procedure
            session = Session.builder.configs({
                "user": self.user,
                "password": self.password,
                "account": self.account,
                "warehouse": self.warehouse,
                "database": self.database,
                "schema": self.schema,
                "role": self.role          #Added to debug
            }).create()
            return session
        except Exception as e:
            print(e)
    
    def call_stored_procedure(self, session, proc_name, param):
        try:
            # Call the stored procedure
            session.call(proc_name, param)
            print(f"Procedure {proc_name} called successfully with param: {param}")
        except Exception as e:
            print(f"Error calling procedure {proc_name}: {e}")
    
    def call_procedures(self, exec_id):
        session = self.create_session()
        print("Calling stored procedures with exec_id: ", exec_id)
        self.call_stored_procedure(session, "MIGRATION_EXECUTION_HEADER", exec_id)
        self.call_stored_procedure(session, "twb_file", exec_id)
        self.call_stored_procedure(session, "Header_table", exec_id)
        self.call_stored_procedure(session, "Detail_table", exec_id)
        self.call_stored_procedure(session, "View_Model", exec_id)
        self.call_stored_procedure(session, "POPULATE_WORKSHEET_HEADER2",exec_id)
        
