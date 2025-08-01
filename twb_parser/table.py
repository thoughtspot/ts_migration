#Import modules and fuctions
import pandas as pd
import numpy as np
from utilities.logger_config import get_logger
class Table():

    def __init__(self):
        pass

    #Getting Table info
    def find_table(self, soup, twb_name):
        try:
            get_logger().info(f'Table parsing started for {twb_name} file')
            data_dict = []
            tag_dict = []
            for datasource in soup.find('datasources').find_all('datasource'):

                if datasource.find('extract'):
                    conversion = 'Mode Dataset'
                else:
                    conversion = 'Thoughtspot table'
                
                if datasource.find('_.fcp.ObjectModelEncapsulateLegacy.false...relation') != None:
                    for connection in datasource.find('named-connections').find_all('named-connection'):
                        connection_tag = connection.find('connection')
                        # create a dict to store datasource name and its connection tag
                        tag_dict.append({
                            datasource.get('name'): connection_tag
                        })

                        relations = datasource.find('_.fcp.ObjectModelEncapsulateLegacy.false...relation').find_all('relation')

                        if relations:
                            for relation in relations:
                                if relation.get('table'):                        
                                    data_dict.append({
                                        'object type': 'table',
                                        'datasource name': datasource.get('caption'),
                                        'table name': relation.get('table'),
                                        'conversion in TS': conversion,        
                                        'property type': 'connection tag',
                                        'property name': '',
                                        'property value': connection_tag,
                                    })
                                elif relation.get('type')=='text':
                                    data_dict.append({
                                        'object type': 'custom sql query',
                                        'datasource name': datasource.get('caption'),
                                        'table name': relation.get('name'),
                                        'property type' : 'text',
                                        'property name' : 'query',
                                        'property value' : relation.text,
                                        'conversion in TS': conversion,           
                                    })
                                    
                                    # Connection tag for custom SQL query
                                    # Compare 'connection' attribute of 'relation' with the 'name' of 'named-connection'
                                    relation_connection = relation.get('connection')
                                    for named_connection in datasource.find_all('named-connection'):
                                        if named_connection.get('name') == relation_connection:
                                            connection_tag = named_connection.find('connection')
                                            data_dict.append({
                                                'object type': 'custom sql query',
                                                'datasource name': datasource.get('caption'),
                                                'table name': relation.get('name'),
                                                'property type': 'connection tag',
                                                'property name': '',
                                                'property value': connection_tag,
                                            })

                        else:
                            relation = datasource.find('_.fcp.ObjectModelEncapsulateLegacy.false...relation')
                            if relation.get('table'):
                                data_dict.append({
                                    'object type': 'table',
                                    'datasource name': datasource.get('caption'),
                                    'table name': relation.get('table'),                    
                                    'conversion in TS': conversion,
                                    'property type': 'connection tag',
                                    'property name': '',
                                    'property value': connection_tag,
                                })
                            elif relation.get('type') == 'text':
                                data_dict.append({
                                    'object type': 'custom sql query',
                                    'datasource name': datasource.get('caption'),
                                    'table name': relation.get('name'),
                                    'property type' : 'text',
                                    'property name' : 'query',
                                    'property value' : relation.text,
                                    'conversion in TS': conversion,           
                                })

                                # Connection tag for custom SQL query
                                relation_connection = relation.get('connection')
                                for named_connection in datasource.find_all('named-connection'):
                                    if named_connection.get('name') == relation_connection:
                                        connection_tag = named_connection.find('connection')
                                        data_dict.append({
                                            'object type': 'custom sql query',
                                            'datasource name': datasource.get('caption'),
                                            'table name': relation.get('name'),
                                            'property type': 'connection tag',
                                            'property name': '',
                                            'property value': connection_tag,
                                        })
                else:
                    relations = datasource.find('_.fcp.ObjectModelEncapsulateLegacy.true...relation').find_all('relation')
                    for connection in datasource.find('named-connections').find_all('named-connection'):
                        connection_tag = connection.find('connection')
                        if relations:
                            for relation in relations:
                                if relation.get('table'): 
                                    data_dict.append({
                                        'object type': 'table',
                                        'datasource name': datasource.get('caption'),
                                        'table name': relation.get('table'),
                                        'conversion in TS': conversion,           
                                        'property type': 'connection tag',
                                        'property name': '',
                                        'property value': connection_tag,  
                                    })
                                elif relation.get('type')=='text':
                                    data_dict.append({
                                        'object type': 'custom sql query',
                                        'datasource name': datasource.get('caption'),
                                        'table name': relation.get('name'),
                                        'property type' : 'text',
                                        'property name' : 'query',
                                        'property value' : relation.text,
                                        'conversion in TS': conversion,           
                                    })
                                    # Connection tag for custom SQL query
                                    relation_connection = relation.get('connection')
                                    for named_connection in datasource.find_all('named-connection'):
                                        if named_connection.get('name') == relation_connection:
                                            connection_tag = named_connection.find('connection')
                                            data_dict.append({
                                                'object type': 'custom sql query',
                                                'datasource name': datasource.get('caption'),
                                                'table name': relation.get('name'),
                                                'property type': 'connection tag',
                                                'property name': '',
                                                'property value': connection_tag,
                                            })
                        else:
                            relation = datasource.find('_.fcp.ObjectModelEncapsulateLegacy.false...relation')
                            if relation.get('table'):
                                data_dict.append({
                                    'object type': 'table',
                                    'datasource name': datasource.get('caption'),
                                    'table name': datasource.find('_.fcp.ObjectModelEncapsulateLegacy.true...relation').get('table'),                    
                                    'conversion in TS': conversion,
                                    'property type': 'connection tag',
                                    'property name': '',
                                    'property value': connection_tag,
                                })
                            elif relation.get('type') == 'text':
                                data_dict.append({
                                    'object type': 'custom sql query',
                                    'datasource name': datasource.get('caption'),
                                    'table name': relation.get('name'),
                                    'property type' : 'text',
                                    'property name' : 'query',
                                    'property value' : relation.text,
                                    'conversion in TS': conversion,           
                                })
                                
                                # Connection tag for custom SQL query
                                relation_connection = relation.get('connection')
                                for named_connection in datasource.find_all('named-connection'):
                                    if named_connection.get('name') == relation_connection:
                                        connection_tag = named_connection.find('connection')
                                        data_dict.append({
                                            'object type': 'custom sql query',
                                            'datasource name': datasource.get('caption'),
                                            'table name': relation.get('name'),
                                            'property type': 'connection tag',
                                            'property name': '',
                                            'property value': connection_tag,
                                        })
                

            df_table = pd.DataFrame.from_dict(data_dict)

            ordered_col = ['twb file name', 'object type', 'datasource name', 'table name', 'remote column name', 'local column name', 
        'worksheet name', 'dashboard name', 'property type', 'property name', 'property value', 'proposed value',
          'conversion in TS', 'supported in TS', 'supported in Migrator', 'exec_error_details']
            col = ['remote column name', 'local column name', 'worksheet name', 'dashboard name', 
        'supported in TS', 'supported in Migrator', 'exec_error_details', 'proposed value']
            df_table[col] = np.NaN
            df_table['twb file name'] = twb_name
            df_table.reset_index(drop=True, inplace=True)
            df_table = df_table.reindex(columns=ordered_col,)
            get_logger().info(f'Table parsing completed for {twb_name} file')
            return df_table
        except Exception as e:
            get_logger().error(f'Error in searching for tables: {e}')
            error_data = {
                'twb file name': twb_name,
                'object type': 'table',
                'exec_error_details': str(e)
            }

            df_table = pd.DataFrame([error_data])
            df_table = df_table.reindex(columns=[
                'twb file name', 'object type', 'datasource name', 'table name', 
                'remote column name', 'local column name', 'worksheet name', 'dashboard name', 'property type', 
                'property name', 'property value', 'proposed value', 'conversion in TS', 
                'supported in TS', 'supported in Migrator', 'exec_error_details'
            ])
            
            return df_table