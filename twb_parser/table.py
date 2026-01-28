#Import modules and fuctions
import pandas as pd
import numpy as np
from utilities.logger_config import get_logger

class Table():

    def __init__(self):
        pass
    #added below private method so that for sql proxies we get dbname isntead of [sqlproxy]
    #change it sqlproxy-dbname 
    def _resolve_table_name(self, relation, connection_tag, fallback_name):
        table_name = relation.get('table') if relation else None
        if connection_tag:
            conn_class = connection_tag.get('class')
            if conn_class == 'sqlproxy':
                if table_name and table_name.strip('[]').lower() == 'sqlproxy':
                    return connection_tag.get('dbname') or fallback_name
        return table_name or fallback_name

    #Getting Table info
    def find_table(self, soup, twb_name):
        try:
            get_logger().info(f'Table parsing started for {twb_name} file')
            data_dict = []
            tag_dict = []
            
            for datasource in soup.find('datasources').find_all('datasource'):
                #added ds_name to handle params
                ds_name = datasource.get('caption') or datasource.get('name') 

                if datasource.find('extract'):
                    conversion = 'Mode Dataset'
                else:
                    conversion = 'Thoughtspot table'
                
                
                # Object Model False
                if datasource.find('_.fcp.ObjectModelEncapsulateLegacy.false...relation') != None:
                    
                    #  Named Connections 
                    if datasource.find('named-connections'): #added check as named-connection mdo not exist for param ,sql views
                        
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
                                            'datasource name': ds_name,
                                            'table name': relation.get('name'),
                                            'conversion in TS': conversion,        
                                            'property type': 'connection tag',
                                            'property name': '',
                                            'property value': connection_tag,
                                        })
                                    elif relation.get('type')=='text':
                                        data_dict.append({
                                            'object type': 'custom sql query',
                                            'datasource name': ds_name,
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
                                                    'datasource name': ds_name,
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
                                        'datasource name': ds_name,
                                        'table name': relation.get('name'),                    
                                        'conversion in TS': conversion,
                                        'property type': 'connection tag',
                                        'property name': '',
                                        'property value': connection_tag,
                                    })
                                elif relation.get('type') == 'text':
                                    data_dict.append({
                                        'object type': 'custom sql query',
                                        'datasource name': ds_name,
                                        'table name': relation.get('name'),
                                        'property type' : 'text',
                                        'property name' : 'query',
                                        'property value' : relation.text,
                                        'conversion in TS': conversion,           
                                    })

                                    relation_connection = relation.get('connection')
                                    for named_connection in datasource.find_all('named-connection'):
                                        if named_connection.get('name') == relation_connection:
                                            connection_tag = named_connection.find('connection')
                                            data_dict.append({
                                                'object type': 'custom sql query',
                                                'datasource name': ds_name,
                                                'table name': relation.get('name'),
                                                'property type': 'connection tag',
                                                'property name': '',
                                                'property value': connection_tag,
                    
                                        })
                    
                    #  False Object -No Named Connections- SQLProxy,params
                    else:
                        connection_tag = datasource.find('connection')
                        relations_tag = datasource.find('_.fcp.ObjectModelEncapsulateLegacy.false...relation')
                        relations = relations_tag.find_all('relation')
                        
                        
                        # if not relations: 
                        #     relations = [relations_tag]

                        for relation in relations:
                            if relation.get('table'):
                                data_dict.append({
                                    'object type': 'table',
                                    'datasource name': ds_name,
                                    'table name': self._resolve_table_name(relation, connection_tag, ds_name),
                                    'conversion in TS': conversion,        
                                    'property type': 'connection tag',
                                    'property name': '',
                                    'property value': connection_tag,
                                })
              
                
                #  Object Model True
                else:
                   
                    if datasource.find('_.fcp.ObjectModelEncapsulateLegacy.true...relation') is not None:
                        
                        relations = datasource.find('_.fcp.ObjectModelEncapsulateLegacy.true...relation').find_all('relation')
                        
                        if not relations:
                             relations = [datasource.find('_.fcp.ObjectModelEncapsulateLegacy.true...relation')]

                        #  Named Connections 
                        if datasource.find('named-connections'):
                            for connection in datasource.find('named-connections').find_all('named-connection'):
                                connection_tag = connection.find('connection')
                                if relations:
                                    for relation in relations:
                                        if relation.get('table'): 
                                            data_dict.append({
                                                'object type': 'table',
                                                'datasource name': ds_name,
                                                'table name': relation.get('name'),
                                                'conversion in TS': conversion,           
                                                'property type': 'connection tag',
                                                'property name': '',
                                                'property value': connection_tag,  
                                            })
                                        elif relation.get('type')=='text':
                                            data_dict.append({
                                                'object type': 'custom sql query',
                                                'datasource name': ds_name,
                                                'table name': relation.get('name'),
                                                'property type' : 'text',
                                                'property name' : 'query',
                                                'property value' : relation.text,
                                                'conversion in TS': conversion,           
                                            })
                                            relation_connection = relation.get('connection')
                                            for named_connection in datasource.find_all('named-connection'):
                                                if named_connection.get('name') == relation_connection:
                                                    connection_tag = named_connection.find('connection')
                                                    data_dict.append({
                                                        'object type': 'custom sql query',
                                                        'datasource name': ds_name,
                                                        'table name': relation.get('name'),
                                                        'property type': 'connection tag',
                                                        'property name': '',
                                                        'property value': connection_tag,
                                                    })
                         
                            else:
                                relation = datasource.find('_.fcp.ObjectModelEncapsulateLegacy.true...relation')
                                if relation.get('table'):
                                    data_dict.append({
                                        'object type': 'table',
                                        'datasource name': ds_name,
                                        'table name': relation.get('name'),                    
                                        'conversion in TS': conversion,
                                        'property type': 'connection tag',
                                        'property name': '',
                                        'property value': connection_tag,
                                    })
                                elif relation.get('type') == 'text':
                                    data_dict.append({
                                        'object type': 'custom sql query',
                                        'datasource name': ds_name,
                                        'table name': relation.get('name'),
                                        'property type' : 'text',
                                        'property name' : 'query',
                                        'property value' : relation.text,
                                        'conversion in TS': conversion,           
                                    })
                                    
                                    relation_connection = relation.get('connection')
                                    for named_connection in datasource.find_all('named-connection'):
                                        if named_connection.get('name') == relation_connection:
                                            connection_tag = named_connection.find('connection')
                                            data_dict.append({
                                                'object type': 'custom sql query',
                                                'datasource name': ds_name,
                                                'table name': relation.get('name'),
                                                'property type': 'connection tag',
                                                'property name': '',
                                                'property value': connection_tag,
                                            })
                        
                        # Case when SQLProxy for True Object model
                        else:
                                connection_tag = datasource.find('connection')
                                for relation in relations:
                                    if relation.get('table'):
                                        data_dict.append({
                                            'object type': 'table',
                                            'datasource name': ds_name,
                                            'table name': self._resolve_table_name(relation, connection_tag, ds_name),
                                            'conversion in TS': conversion,           
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