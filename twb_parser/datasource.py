#Importing necessary modules
import pandas as pd
import numpy as np
from utilities.logger_config import get_logger
class Datasource():

    def __init__(self):
        pass


    def get_datasource(self, soup, twb_name, datasource_map, join_map, live_flag):

        try:
            get_logger().info(f'Datasource parsing started for {twb_name} file')
            data_dict = []

            for datasource in soup.find('datasources').find_all('datasource'):          
                get_logger().info(f'Processing datasource properties for {datasource["caption"]} datasource')                 
                # ds_Type
                if (datasource.find('extract')):
                    
                    ds_type_twb = 'Extract'
                    if live_flag == True:
                        ds_type = 'Live'
                        conversion = 'Thoughtspot table'    
                    else:
                        conversion = 'Mode Dataset'
                        ds_type = ds_type_twb
                    data_dict.append({
                        'object type': 'datasource property',
                        'datasource name': datasource['caption'],
                        'property type': 'ds_type',
                        'property name': 'connection',
                        'property value': ds_type_twb,
                        'proposed value': ds_type,
                        'conversion in TS': conversion,
                        'supported in TS': 'Full',
                        'supported in Migrator': 'Full'
                    })
                else:
                    ds_type = 'Live'
                    conversion = 'Thoughtspot table'
                    data_dict.append({
                        'object type': 'datasource property',
                        'datasource name': datasource['caption'],
                        'property type': 'ds_type',
                        'property name': 'connection',
                        'property value': ds_type,
                        'proposed value': ds_type,
                        'conversion in TS': conversion,
                        'supported in TS': 'Full',
                        'supported in Migrator': 'Full'
                    })
                #datasource name
                if datasource.find('named-connection').find('connection'):
                    for connection in datasource.find('named-connections').find_all('named-connection'):
                        connection = connection.find('connection').get('class')
                        data_dict.append({
                            'object type': 'datasource property',
                            'datasource name': datasource['caption'],
                            'property type': 'named_connection',
                            'property name': 'connector',
                            'property value': connection
                        })

                # datasource property (Joins and filters)
                relations = datasource.find('_.fcp.ObjectModelEncapsulateLegacy.false...relation')
                relation_join = relations.get('join')
                for clause in relations.find_all('clause'):
                    join_tbls = [exp.get('op') for exp in clause.find_all('expression') if exp.get('op') != '=']
                    data_dict.append(
                        {
                            'object type': 'datasource property',
                            'datasource name': datasource['caption'],
                            'property type': 'join',
                            'property name': relation_join,
                            'property value': "::".join(join_tbls)
                        })      
                    relation_join = relations.find('relation').get('join')
                
            df_datasource = pd.DataFrame.from_dict(data_dict)
        
            ordered_col = ['twb file name', 'object type', 'datasource name', 'table name',	'remote column name', 'local column name',
       'worksheet name', 'dashboard name', 'property type',	'property name', 'property value', 'proposed value','conversion in TS', 'supported in TS', 'supported in Migrator','exec_error_details']
            
            col = ['table name', 'remote column name', 'local column name',
       'worksheet name', 'dashboard name', 'exec_error_details']
            df_datasource[col] = np.nan
            df_datasource['twb file name'] = twb_name
            df_datasource.reset_index(drop=True, inplace=True)
            df_datasource = df_datasource.reindex(columns=ordered_col,)

            # datasource mapping
            get_logger().info(f'Processing datasource mapping for {twb_name} file')
            flag_mapping = df_datasource.loc[df_datasource['property type'] == 'ds_type', ['datasource name', 'proposed value']]
            flag_mapping = flag_mapping.set_index('datasource name')['proposed value'].to_dict()

            df_datasource['flag'] = df_datasource['datasource name'].map(flag_mapping)
            cols = list(df_datasource.columns)
            property_value_index = cols.index('proposed value')
            cols.insert(property_value_index + 1, cols.pop(cols.index('flag')))
            df_datasource = df_datasource[cols]

            df_named_connection = df_datasource[df_datasource['property type'] == 'named_connection'].reset_index(drop=True)

            datasource_map['Name'] = datasource_map['Name'].str.lower()
            df_named_connection = pd.merge(df_named_connection, datasource_map, left_on='property value', right_on='Name', how='left')

            df_named_connection['conversion in TS'] = df_named_connection['Name']
            df_named_connection['supported in TS'] = np.where(
                df_named_connection['flag'].str.lower() == 'live', df_named_connection['Supported by Thoughtspot'],
                np.where(df_named_connection['flag'].str.lower() == 'extract', df_named_connection['Supported by Mode'], np.nan)
            )
            df_named_connection['supported in Migrator'] = df_named_connection['supported in TS']
            df_named_connection.drop(columns=['Name', 'flag','Supported by Thoughtspot', 'Supported by Mode', 'supported by TS migrator'], inplace=True)

            df_datasource = df_datasource[df_datasource['property type'] != 'named_connection']
            df_datasource = pd.concat([df_datasource, df_named_connection], ignore_index=True)
            df_datasource = df_datasource.reindex(columns=ordered_col,)
            df_datasource.reset_index(drop=True, inplace=True)
            get_logger().info(f'Processing datasource mapping for {twb_name} file completed')

            # Join mapping
            join_df = df_datasource[df_datasource['property type'] == 'join'].reset_index(drop=True)
            join_map['Tableau Join'] = join_map['Tableau Join'].str.lower()

            join_df = pd.merge(join_df, join_map, left_on='property name', right_on='Tableau Join', how='left')
            join_df['conversion in TS'] = join_df['TS Join']
            join_df['supported in TS'] = join_df['Supported by Thoughtspot']
            join_df['supported in Migrator'] = join_df['supported by TS migrator']
            join_df.drop(['Tableau Join', 'TS Join', 'Supported by Thoughtspot', 'supported by TS migrator', 'Supported in mode'], axis=1, inplace=True)
            
            df_datasource = df_datasource[df_datasource['property type'] != 'join']
            df_datasource = pd.concat([df_datasource, join_df], ignore_index=True)
            df_datasource.reset_index(drop=True, inplace=True)
            get_logger().info(f'Datasourcing parsing and mapping for {twb_name} file completed')

            return df_datasource
        
        except Exception as e:
            get_logger().error(f'Error in get_datasource: {e}')
            error_data = {
                'twb file name': twb_name,
                'object type': 'datasource property',
                'exec_error_details': str(e)
            }

            # Initialize DataFrame with default NaN values
            df_datasource = pd.DataFrame([error_data])
            df_datasource = df_datasource.reindex(columns=[
                'twb file name', 'object type', 'datasource name', 'table name', 'remote column name',
                'local column name', 'worksheet name', 'dashboard name', 'property type', 
                'property name', 'property value','proposed value', 'conversion in TS', 
                'supported in TS', 'supported in Migrator', 'exec_error_details'
            ])
            return df_datasource