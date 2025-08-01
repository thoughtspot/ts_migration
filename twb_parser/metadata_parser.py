#Import necessary modules and functions
import hashlib
from datetime import datetime
from .datasource import Datasource
# from datatype import Datatype
from .table import Table
from .column import Column
from .chart_properties import Chart_properties
#from chart_filter import *
from .dashboard import Dashboard
from .dashboard_property import Dashboard_property
from .filters import Filters
from utilities import file_reader, data_ingest
import os
import pandas as pd
from bs4 import BeautifulSoup
import warnings
warnings.simplefilter(action='ignore', category=Warning)
from .column_datatype_mapping import column_datatype
from utilities import data_ingest
from utilities.logger_config import get_logger
# import xmltodict

class MetadataExtraction():

    def __init__(self, twb_dir=None, output_dir=None,):
        
        self.execution_start_time = datetime.now()
        self.dashboard_property_instance = Dashboard_property()
        self.datasource_instance = Datasource()
        # self.datatype_instance = Datatype()
        self.table_instance = Table()   
        self.column_instance = Column()
        self.chart_properties_instance = Chart_properties()
        self.filters_instance = Filters()
        self.dashboard_instance = Dashboard()
        self.data_ingest_instance = data_ingest.DataIngress()
        self.column_datatype_instance = column_datatype()
        self.twb_files = file_reader.get_twb_files(twb_dir)
        self.output_dir = output_dir
        data_csv = file_reader.get_mapping_file()
        self.datatype_map =  data_csv['datatype.csv']
        self.datasource_map = data_csv['datasource.csv']
        self.join_map = data_csv['joins.csv']
        self.chart_mapping = data_csv['chart_prop.csv']
        self.filter_mapping = data_csv['object_feature_filter.csv']
        self.dashboard_mapping_df = data_csv['object_feature_dashboard.csv']

    # Get the Soup:
    def soup_twb(self, twb):
        try:
            # Read the twb File :
            file = open(twb, "r")
            contents = file.read()
            soup = BeautifulSoup(contents, 'xml')
            #dashboard_soup=soup.find('dashboards').find_all('dashboard')
            return soup
        except Exception as e:
            get_logger().error(f'Error in reading twb file: {e}')
            print(e)

    def dataextract(self,twb_file, flag, filter_mapping,dashboard_mapping_df, datasource_map, chart_mapping, join_map, datatype_map):
        soup = self.soup_twb(twb_file)
        head, tail = os.path.split(twb_file)
        twb = tail
        df_datasource = self.datasource_instance.get_datasource(soup, twb_name=twb, datasource_map=datasource_map, join_map=join_map, live_flag=flag)
        df_table=self.table_instance.find_table(soup,twb_name=twb)
        df_column= self.column_instance.find_column(soup, twb_name=twb)
        df_calculated=self.column_instance.find_calculated(soup, twb_name=twb)
        df_chart_mapped=self.chart_properties_instance.getting_chart_df(soup, twb_name=twb, chart_prop_mapping=chart_mapping)
        datasource_filter= self.filters_instance.datasource_level_filter(soup,filter_mapping, twb_name=twb, live_flag=flag)
        filters_chart= self.filters_instance.chart_level_filter(soup, filter_mapping, twb_name=twb, live_flag=flag)
        dashboard_df= self.dashboard_instance.find_dashboard(soup, twb_name=twb)
        df_dashboard_property= self.dashboard_property_instance.dashboard_property(soup,dashboard_mapping_df, twb_name=twb)
        result_df = pd.concat([df_datasource, df_table, df_column, df_calculated, df_chart_mapped, datasource_filter, filters_chart, dashboard_df, df_dashboard_property], ignore_index=True)
        # since columns are processed separately without datasource properties so have created a separate mapping module for column datatype mapping
        result_df = self.column_datatype_instance.column_datatype_mapping(datatype_map, result_df)
        
        result_df['exec_mode'] = 'command_line'
        if result_df.empty:
            result_df['exec_status'] = 'failed'
        else:
            result_df['exec_status'] = 'success'
        result_df.reset_index(drop=True, inplace=True)
        return result_df


    def start_exe(self,flag):
        # Combine results from all TWB files
        combined_result = pd.DataFrame()
        
        for twb in self.twb_files:
            print(twb)
            # result = self.dataextract(twb, self.datasource_map, self.join_map, self.chart_mapping, self.filter_mapping, self.dashboard_mapping_df)
            result = self.dataextract(twb, flag,self.filter_mapping, self.dashboard_mapping_df, self.datasource_map, self.chart_mapping, self.join_map, self.datatype_map)
            combined_result = pd.concat([combined_result, result], ignore_index=True)
        combined_result['exec_start_time'] = self.execution_start_time
        execution_stop_time = datetime.now()
        combined_result['exec_end_time'] = execution_stop_time
        combined_result['exec_id'] = combined_result['exec_mode'].astype(str) + combined_result['exec_start_time'].astype(str) + combined_result['exec_end_time'].astype(str)
        combined_result['exec_id'] = combined_result['exec_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
        combined_result['output_file_type'] = ''
        combined_result['output_file_name'] = ''
        output_dump = combined_result.copy(deep=True)
        output_dump['last_ingested_date'] = datetime.now()
        if self.output_dir[-1] != '/':
            self.output_dir = self.output_dir + '/'
        
        # # output_dump.to_csv(self.output_dir+'combined_output.csv',index=False)     
        # # Define the output file path
        # output_file = self.output_dir + 'combined_output.csv' 
        #     # Check if the file exists
        # if os.path.isfile(output_file):
        #     # Append data to the existing file without writing the header
        #     output_dump.to_csv(output_file, mode='a', header=False, index=False)
        # else:
        #     # Write data to a new file including the header
        #     output_dump.to_csv(output_file, mode='w', header=True, index=False)

        # Load the data into Snowflake
        # self.data_ingest_instance.write_dump_data(output_dump)

        return output_dump
    
