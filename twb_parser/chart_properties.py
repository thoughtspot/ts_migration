#Import necessary modules and functions
import pandas as pd
import re
import numpy as np
from utilities.logger_config import get_logger
class Chart_properties():

    def __init__(self):
        pass

    def getting_chart_df(self, soup, twb_name, chart_prop_mapping):
        
            # Charts 
            data_dict = []
            debug_list = []
            try:
            # iterate throught worksheets
                get_logger().info(f'Chart properties parsing started for {twb_name} file')
                for worksheet in soup.find('worksheets').find_all('worksheet'):
                    get_logger().info(f'Processing chart properties for {worksheet["name"]} worksheet')
                    
                    # iterate through panes
                    try:   
                        for pane in worksheet.find('table').find('panes').find_all('pane'):    
                            data_dict.append({
                                'object type': 'chart',
                                # there's an assumption that there can be more than one datasource in a worksheet, 
                                # at that time we have to alter this code
                                'datasource name': worksheet.find_all('datasources')[0].find_all('datasource')[0]['caption'],
                                'worksheet name': worksheet['name'],
                                'property type': 'chart type',
                                'property name': 'mark',
                                'property value' : pane.find('mark')['class'],

                            })
                            # Encoding details
                            try:
                                for encoding in pane.find('encodings').find_all():
                                    for attr_name, attr_value in encoding.attrs.items():
                                        
                                        extract_value = lambda x: re.search(r'\.\[(.*?)\]', x).group(1) if re.search(r'\.\[(.*?)\]', x) else None

                                        data_dict.append({
                                            'object type': 'chart property',
                                            # there's an assumption that there can be more than one datasource in a worksheet, 
                                            # at that time we have to alter this code            
                                            'datasource name': worksheet.find_all('datasources')[0].find_all('datasource')[0]['caption'],
                                            'worksheet name': worksheet['name'],
                                            'property type': 'encoding',
                                            'property name': encoding.name,
                                            'property value': extract_value(attr_value,)
                                        })
                            except:
                                # raise TypeError(f'Encoding details not found in the {worksheet["name"]} worksheet')
                                get_logger().error(f'Encoding details not found in the {worksheet["name"]} worksheet')
                                pass
                        
                        get_logger().info(f'Chart property parsing Axis details completed for {worksheet["name"]} worksheet')
                        # Axis Details
                        table = worksheet.find('table')
                        extract_value = lambda x: re.search(r'\.\[(.*?)\]', x).group(1) if re.search(r'\.\[(.*?)\]', x) else None
                        if (table.find('rows') != None):
                            rows = table.find('rows').text if table.find('rows') else None
                            # rows_list = [extract_value(row) for row in (rows.replace('+', '/').split('/') if '/' in rows or '+' in rows else [rows])]
                            rows_list = [extract_value(row) for row in (rows.replace('+','/').split('/') if '/' in rows or '+' in rows else [rows])]
                            for row in rows_list:
                                data_dict.append({
                                    'object type': 'chart property',
                                    'datasource name': worksheet.find_all('datasources')[0].find_all('datasource')[0]['caption'],
                                    'worksheet name': worksheet['name'],
                                    'property type': 'axis',
                                    'property name': 'rows',
                                    'property value': row
                                })
                        if (table.find('cols') != None):
                            cols = table.find('cols').text if table.find('cols') else None
                            cols_list = [extract_value(col) for col in (cols.replace('+','/').split('/') if '/' in cols or '+' in cols else [cols])]
                            for col in cols_list:
                                data_dict.append({
                                    'object type': 'chart property',
                                    'datasource name': worksheet.find_all('datasources')[0].find_all('datasource')[0]['caption'],
                                    'worksheet name': worksheet['name'],
                                    'property type': 'axis',
                                    'property name': 'columns',
                                    'property value': col
                                })

                        get_logger().info(f'Chart property parsing Datasource details completed for {worksheet["name"]} worksheet')
                        # Datasource Details
                        for datasources in worksheet.find_all('datasources'):
                            for datasource_name in datasources.find_all('datasource'):
                                data_dict.append({
                                    'object type': 'chart property',
                                    'datasource name': datasource_name['caption'],
                                    'worksheet name': worksheet['name'],
                                    'property type': 'datasource',
                                    'property name': 'name',
                                    'property value': datasource_name['caption']
                                }) 
                
                    except Exception as e:
                        get_logger().error(f'An error occured in getting chart properties: {str(e)}')


                    df_chart = pd.DataFrame.from_dict(data_dict)

                    ordered_col = ['twb file name', 'object type', 'datasource name', 'table name', 'remote column name', 'local column name',
                'worksheet name', 'dashboard name', 'property type', 'property name', 'property value', 'proposed value','conversion in TS', 
                'supported in TS', 'supported in Migrator', 'exec_error_details']
                        
                    col = ['table name', 'remote column name', 'local column name', 'dashboard name', 'conversion in TS',
                        'supported in TS', 'supported in Migrator', 'exec_error_details', 'proposed value']
                    df_chart[col] = np.NaN
                    df_chart['twb file name'] = twb_name
                    df_chart.reset_index(drop=True, inplace=True)
                    df_chart = df_chart.reindex(columns=ordered_col,)

                    get_logger().info(f'Chart properties Chart(mark) mapping started for {twb_name} file')
                    # Chart mapping
                    df_mark = df_chart[(df_chart['object type'] == 'chart') & (df_chart['property name'] == 'mark')]
                    df_mark.reset_index(drop=True, inplace=True)
                    df_mark['property value'] = df_mark['property value'].str.lower()
                    chart_mark_mapping = chart_prop_mapping[chart_prop_mapping['Type'] == 'mark']
                    chart_mark_mapping['tableau chart property'] = chart_mark_mapping['tableau chart property'].str.lower()
                    df_mark = df_mark.merge(chart_mark_mapping, left_on='property value', right_on='tableau chart property', how='left')
                    df_mark['conversion in TS'] = df_mark['TS chart property']
                    df_mark['supported in TS'] = df_mark['Supported by Thoughtspot']
                    df_mark.drop(['Type','tableau chart property', 'Condition','TS chart property', 'Supported by Thoughtspot', 'Proceed with checks'], axis=1,inplace=True)
                    df_mark.reset_index(drop=True, inplace=True)
                    df_chart = df_chart[df_chart['property name'] != 'mark']
                    df_chart = pd.concat([df_chart, df_mark], ignore_index=True)
                    df_chart.reset_index(drop=True, inplace=True)
                    get_logger().info(f'Chart properties mapping completed for {twb_name} file')

                    # Encodings mapping
                    get_logger().info(f'Chart properties Econding mapping started for {twb_name} file')
                    df_enco = df_chart[df_chart['property type'] == 'encoding']
                    df_enco.reset_index(drop=True, inplace=True)
                    df_enco = df_enco[df_enco['property type']=='encoding'].reset_index(drop=True)
                    df_enco['property name'] = df_enco['property name'].str.lower()
                    chart_prop_mapping = chart_prop_mapping[chart_prop_mapping['Type'] == 'encoding']
                    chart_prop_mapping['tableau chart property'] = chart_prop_mapping['tableau chart property'].str.lower()
                    df_enco = df_enco.merge(chart_prop_mapping, left_on='property name', right_on='tableau chart property', how='left')
                    df_enco['conversion in TS'] = df_enco['TS chart property']
                    df_enco['supported in TS'] = df_enco['Supported by Thoughtspot']
                    df_enco.drop(['Type','tableau chart property', 'Condition','TS chart property', 'Supported by Thoughtspot', 'Proceed with checks'], axis=1, inplace=True)
                    df_enco.reset_index(drop=True, inplace=True)
                    
                    df_chart = df_chart[df_chart['property type'] != 'encoding']
                    df_chart = pd.concat([df_chart, df_enco], ignore_index=True)
                    df_chart.reset_index(drop=True, inplace=True)
                    
                    # Axis Mapping
                    # Function to handle quantitative and measure conditions
                    def check_conditions(group):
                        # Extract rows and columns values
                        rows_value = group[group['property name'] == 'rows']['property value']
                        columns_value = group[group['property name'] == 'columns']['property value']
                        
                        rows_value = rows_value.iloc[0] if len(rows_value) > 0 else None
                        columns_value = columns_value.iloc[0] if len(columns_value) > 0 else None
                        
                        # 1. Missing rows/columns
                        has_rows = not pd.isna(rows_value) and rows_value != 'None'
                        has_columns = not pd.isna(columns_value) and columns_value != 'None'
                        
                        if not has_rows or not has_columns:
                            group['supported in TS'] = 'NoSup'
                            group['conversion in TS'] = 'Value for only row/column available. Can be represented as table or single value KPI'
                            return group
                        
                        # 2. Quantitative values in property value
                        if (rows_value and (rows_value.startswith(('none:', 'usr:')) and rows_value.endswith((':nk')) )) and \
                        (columns_value and (columns_value.startswith(('none:', 'usr:')) and columns_value.endswith(':nk') )):
                            group['supported in TS'] = 'NoSup'
                            group['conversion in TS'] = 'At least one measure and attribute or date field required to plot chart. Can be represented as table'
                            return group
                        elif (rows_value and (rows_value.startswith(('none:', 'usr:', 'yr:', 'qr:', 'sum:','avg:','mn:','tmn:','ctd:')) and rows_value.endswith((':qk', ':ok')))) and \
                        (columns_value and (columns_value.startswith(('none:', 'usr:', 'yr:', 'qr:', 'sum:','avg:','mn:','tmn:','ctd:')) and columns_value.endswith((':qk', ':ok')))):
                            group['supported in TS'] = 'NoSup'
                            group['conversion in TS'] = 'Relevant charts can be created based on the Mark type'
                            return group
                        
                        # 3. Measure values in property value
                        if (rows_value and (rows_value.startswith(('sum:', 'avg:', 'count:', 'yr:', 'qr:', 'mn:','tmn:','ctd:')) and  rows_value.endswith((':qk', ':ok')))) and \
                        (columns_value and (columns_value.startswith(('sum:', 'avg:', 'count:', 'yr:', 'qr:', 'mn:','tmn:','ctd:')) and  columns_value.endswith(':qk', ':ok'))):
                            group['supported in TS'] = 'NoSup'
                            group['conversion in TS'] = 'At least one measure and attribute or date field required to plot chart. Can be represented as table'
                            return group
                        
                        # 4. Else based on the mark type we can create a relevant chart
                        else:
                            group['supported in TS'] = 'Partial'
                            group['conversion in TS'] = 'Relevant charts can be created based on the Mark type'
                        
                        return group

                    # Handle Latitude/Longitude (Condition 4)
                    def update_lat_long(row):
                        if pd.isna(row['property value']) or row['property value'] in [None, 'None']:
                            return row['supported in TS'], row['conversion in TS']
                        
                        if row['property value'] in ['Latitude (generated)', 'Longitude (generated)']:
                            return 'partial', 'Use geo charts to represent the data.'
                        
                        return row['supported in TS'], row['conversion in TS']

                    # Apply condition check for each worksheet
                    get_logger().info(f'Chart properties logic for handling Attributes and Dimensions in rows and columns started for {twb_name} file')
                    df_chart = df_chart.groupby(['twb file name', 'worksheet name']).apply(check_conditions)
                    get_logger().info(f'Chart properties logic for handling Attributes and Dimensions in rows and columns completed for {twb_name} file')

                    get_logger().info(f'Chart properties logic for handling Latitude and Longitude started for {twb_name} file')
                    # Apply Latitude/Longitude condition
                    df_chart[['supported in TS', 'conversion in TS']] = df_chart.apply(
                        lambda row: pd.Series(update_lat_long(row)),
                        axis=1
                    )
                    get_logger().info(f'Chart properties logic for handling Latitude and Longitude completed for {twb_name} file')
                    df_chart = df_chart.reset_index(level=[0,1], drop=True)
                    df_chart.reset_index(drop=True, inplace=True)
                    get_logger().info(f'Chart properties parsing completed for {twb_name} file')
                return df_chart
        
            except Exception as e:        
                
                get_logger().error(f'An error occured in getting chart properties: {str(e)}')

                error_data = {
                'twb file name': twb_name,
                'object type': 'chart property',
                'exec_error_details': "chart property error" + str(e),
                }

                # Initialize DataFrame with default NaN values
                df_chart = pd.DataFrame([error_data])
                df_chart = df_chart.reindex(columns=[
                    'twb file name', 'object type', 'datasource name', 'table name', 
                    'remote column name', 'local column name', 'worksheet name', 'dashboard name', 'property type', 
                    'property name', 'property value', 'proposed value','conversion in TS', 
                    'supported in TS', 'supported in Migrator', 'exec_error_details'
                ])
                
                return df_chart

                    
                    


        
