import numpy as np
import pandas as pd
from utilities.logger_config import get_logger
class column_datatype:

    def __init__(self):
        pass

    def column_datatype_mapping(self, datatype_map, result_df):
        try:            
            get_logger().info(f'Column datatype mapping started')
            dc = result_df[result_df['object type'].isin(['column', 'datasource property'])]
            dc.reset_index(drop=True, inplace=True)
            col_df = dc[dc['property type'].isin(['metadata-record','ds_type'])].reset_index(drop=True)

            flag_mapping = dc.loc[dc['property type'] == 'ds_type', ['datasource name', 'proposed value']]
            flag_mapping = flag_mapping.set_index('datasource name')['proposed value'].to_dict()

            col_df['flag'] = col_df['datasource name'].map(flag_mapping)
            cols = list(col_df.columns)
            property_value_index = cols.index('property value')
            cols.insert(property_value_index + 1, cols.pop(cols.index('flag')))
            col_df = col_df[cols]
            
            col_df = col_df[col_df['property type'] == 'metadata-record'].reset_index(drop=True)

            col_df['property value'] = col_df['property value'].str.lower()
            col_df = col_df.merge(datatype_map, left_on='property value', right_on='Tableau Data Type', how='left')
            col_df['flag'] = col_df['flag'].astype(str)
            col_df['flag'] = col_df['flag'].str.lower()
            col_df['conversion in TS'] = np.where(
                col_df['flag'].str.lower() == 'live', col_df['TS Data Type'],
                np.where(col_df['flag'].str.lower() == 'extract', col_df['Mode Data Type'], np.nan)
            )
            col_df['supported in TS'] = col_df['supported by TS']
            col_df['supported in Migrator'] = col_df['supported by TS']
            col_df.drop(['flag', 'Datatype','Tableau Data Type', 'TS Data Type', 'Mode Data Type', 'Tableau Role', 'TS Column Type', 'TS Aggregation Type',
                         'TS Geo Config','supported by TS', 'supported by Migrator'], axis=1, inplace=True)
            col_df.reset_index(drop=True, inplace=True)

            result_df = result_df[result_df['property type'] != 'metadata-record']
            
            result_df = pd.concat([result_df, col_df], ignore_index=True)
            # result_df.to_csv('column.csv')
            get_logger().info(f'Column datatype mapping completed')
            return result_df
        
        except Exception as e:
            get_logger().error(f'Error in column_datatype_mapping: {e}')
            error_data = {
                'object type': 'column',
                'exec_error_details': str(e)
            }
            
            result_df = pd.DataFrame([error_data])
            result_df = result_df.reindex(columns=[
                'twb file name', 'object type', 'datasource name', 'table name', 
                'remote column name','local column name', 'worksheet name', 'dashboard name', 'property type', 
                'property name', 'property value', 'conversion in TS', 
                'supported in TS', 'supported in Migrator', 'exec_error_details'
            ])

            return result_df