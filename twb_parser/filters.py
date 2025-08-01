import numpy as np 
import pandas as pd 
from collections import deque
import re
from utilities.logger_config import get_logger

class Filters():

    def __init__(self):
        pass

    def datasource_level_filter(self,soup, filter_mapping, twb_name, live_flag):

        filter_df_list = []

        get_logger().info(f"Parsing datasource level filters from '{twb_name}")
        
        # Iterate through datasources to find filters
        for datasource in soup.find('datasources').find_all('datasource'):

            get_logger().info(f"Extracting filters from datasource '{datasource.get('caption')}'")
            
            supported_in_ts_column_name = None
            supported_in_mig_column_name = None

            # Based on flag update the these
            if datasource.find('extract'):
                if live_flag:
                    supported_in_ts_column_name = 'supported in TS'
                    supported_in_mig_column_name = 'supported in Live Migrator'
                else:
                    supported_in_ts_column_name = 'supported in MODE'
                    supported_in_mig_column_name = 'supported in Extract Migrator'
            else:
                supported_in_ts_column_name = 'supported in TS'
                supported_in_mig_column_name = 'supported in Live Migrator'

            filters = datasource.find_all('filter')
            filter_number = 1

            if not filters or len(filters)==0:
                get_logger().info(f"No filters present in the worksheet '{datasource.get('caption')}'")

            # Iterate through the filters
            for filter in filters:

                try :

                    curr_filter_name = filter_number

                    get_logger().info(f"Processing 'Filter_{curr_filter_name}'")

                    # Get the values of class and column
                    filter_class = filter.get('class')
                    filter_column = filter.get('column')

                    # This condition only processes categorical filters
                    if filter_class == 'categorical':
                        user_ui_enumeration = None
                        members = []
                        top_dict = {}
                        wildcard_dict = {}
                        condition_dict = {}
                        general_dict = {}
                        isFunctionToExcept = False

                        # Append the first group filter to the queue
                        queue = deque(filter.find_all('groupfilter', recursive=False))

                        # Process the group filters hierarchically
                        while queue:
                            groupfilter = queue.popleft()
                            function = groupfilter.get('function')

                            if function == 'end' :   # if the filter is top, append values to top dictionary
                                top_dict['function'] = 'top'
                                top_dict['count'] = str(groupfilter.get('count'))
                                top_dict['end'] = groupfilter.get('end')
                            elif function == 'order':   # if the filter is order, append values to top dictionary
                                top_dict['direction'] = groupfilter.get('direction')
                                top_dict['expression'] = groupfilter.get('expression')
                            elif function == 'filter': # if the filter is filter, append values to wildcard/condition dictionary
                                expression = groupfilter.get('expression')
                                if groupfilter.get('user:ui-pattern_type') or groupfilter.get('user:ui-pattern_text'):
                                    wildcard_dict['expression'] = expression
                                    wildcard_dict['user:ui-enumeration'] = groupfilter.get('user:ui-enumeration')
                                    wildcard_dict['user:ui-pattern_type'] = groupfilter.get('user:ui-pattern_type')
                                    wildcard_dict['user:ui-pattern_text'] = groupfilter.get('user:ui-pattern_text')
                                    wildcard_dict['function'] = 'wildcard'
                                else:
                                    condition_dict['function'] = 'condition'
                                    condition_dict['expression'] = expression
                            # All the other functions belongs to general filter
                            elif function == 'union' and groupfilter.get('user:ui-enumeration') == 'inclusive': 
                                user_ui_enumeration = 'inclusive'
                            elif function == 'except' and groupfilter.get('user:ui-enumeration') == 'inclusive':
                                user_ui_enumeration = 'exclusive'
                            elif function == 'except' and groupfilter.get('user:ui-enumeration') == 'exclusive':
                                user_ui_enumeration = 'exclusive'
                            elif function == 'except':
                                isFunctionToExcept = True
                            elif function == 'member' and groupfilter.get('user:ui-enumeration') == 'inclusive':
                                user_ui_enumeration = 'inclusive'
                                members.append(groupfilter.get('member'))
                            elif function == 'member' and groupfilter.get('user:ui-enumeration') == 'exclusive':
                                user_ui_enumeration = 'exclusive'
                                members.append(groupfilter.get('member'))
                            elif function == 'member':
                                members.append(groupfilter.get('member'))
                            elif function == 'empty-level':
                                user_ui_enumeration = groupfilter.get('user:ui-enumeration')
                            elif function == 'range': 
                                from_value = groupfilter.get('from')
                                to_value = groupfilter.get('to')
                                for i in range(int(from_value), int(to_value) + 1):
                                    members.append(str(i))

                            # Append the child group filters back to the queue
                            for gf in groupfilter.find_all('groupfilter', recursive=False):
                                queue.append(gf)

                        # If the case is to except everything except few values
                        if isFunctionToExcept is True and user_ui_enumeration == 'exclusive':
                            user_ui_enumeration = 'inclusive'

                        if user_ui_enumeration:
                            general_dict['user:ui-enumeration'] = user_ui_enumeration
                            general_dict['members'] = members

                        #  Append the common values to all dictionary
                        ordered_values = ['class', 'function', 'column',
                                          'user:ui-enumeration','user:ui-pattern_type',
                                          'user:ui-pattern_text','expression','members',
                                          'count','end','direction']
                        if top_dict:
                            top_dict['class'] = 'categorical'
                            top_dict['column'] = filter_column
                            top_dict = {key: top_dict[key] for key in ordered_values if key in top_dict}
                        if general_dict:
                            general_dict['class'] = 'categorical'
                            general_dict['function'] = 'general'
                            general_dict['column'] = filter_column
                            general_dict = {key: general_dict[key] for key in ordered_values if key in general_dict}
                        if condition_dict:
                            condition_dict['class'] = 'categorical'
                            condition_dict['column'] = filter_column
                            condition_dict = {key: condition_dict[key] for key in ordered_values if key in condition_dict}
                        if wildcard_dict:
                            wildcard_dict['class'] = 'categorical'
                            wildcard_dict['column'] = filter_column
                            wildcard_dict = {key: wildcard_dict[key] for key in ordered_values if key in wildcard_dict}

                        rows= []

                        # For general dictionary, get the supported in mode and migrator values 
                        # from the csv and form them as a dataframe to put them in the dump
                        if general_dict:

                            tableau_class = general_dict['class']
                            func = general_dict['function']
                            enumeration = np.nan
                            if 'user:ui-enumeration' in general_dict:
                                enumeration = general_dict['user:ui-enumeration']
                            supported = filter_mapping[
                                (filter_mapping['tableau_class'] == tableau_class) &
                                (filter_mapping['function'] == func) &
                                (filter_mapping['ui-enumeration'].isna() if pd.isna(enumeration) 
                                else filter_mapping['ui-enumeration'] == enumeration)]
                            supported_in_ts =  supported[supported_in_ts_column_name].values[0] if not supported[supported_in_ts_column_name].empty else None
                            supported_in_migrator = supported[supported_in_mig_column_name].values[0] if not supported[supported_in_mig_column_name].empty else None
                            ts_equivalent = supported['datasource filter hint'].values[0] if not supported['datasource filter hint'].empty else None
                            for key, value in general_dict.items():
                                if key == 'members':
                                    for v in value:
                                        row_df = pd.DataFrame([{'object type':'datasource property' , 
                                                        'datasource name':datasource.get('caption'),
                                                        'property type': 'Filter_' + str(filter_number), 
                                                        'property name': 'member', 'property value': v,
                                                        'supported in TS': supported_in_ts,
                                                        'supported in Migrator' : supported_in_migrator,
                                                        'conversion in TS' : ts_equivalent}])
                                        rows.append(row_df)
                                else :
                                    row_df = pd.DataFrame([{'object type':'datasource property' , 
                                                            'datasource name':datasource.get('caption'),
                                                            'property type': 'Filter_' + str(filter_number), 
                                                            'property name': key, 'property value': value,
                                                            'supported in TS': supported_in_ts,
                                                            'supported in Migrator' : supported_in_migrator,
                                                            'conversion in TS' : ts_equivalent}])
                                    rows.append(row_df)
                            filter_number += 1

                        # Form the dataframes to put them in a dump
                        for d in [d for d in [top_dict, condition_dict, wildcard_dict] if d]:

                            tableau_class = d['class']
                            func = d['function']
                            pattern = np.nan
                            if 'user:ui-pattern_type' in d:
                                pattern = d['user:ui-pattern_type']
                            enumeration = np.nan
                            if 'user:ui-enumeration' in d:
                                enumeration = d['user:ui-enumeration']
                            supported = filter_mapping[
                                (filter_mapping['tableau_class'] == tableau_class) &
                                (filter_mapping['function'] == func) &
                                (filter_mapping['pattern-text'].isna() if pd.isna(pattern) 
                                else filter_mapping['pattern-text'] == pattern) &
                                (filter_mapping['ui-enumeration'].isna() if pd.isna(enumeration) 
                                else filter_mapping['ui-enumeration'] == enumeration)]
                            supported_in_ts =  supported[supported_in_ts_column_name].values[0] if not supported[supported_in_ts_column_name].empty else None
                            supported_in_migrator = supported[supported_in_mig_column_name].values[0] if not supported[supported_in_mig_column_name].empty else None
                            ts_equivalent = supported['datasource filter hint'].values[0] if not supported['datasource filter hint'].empty else None
                            for key, value in d.items():
                                row_df = pd.DataFrame([{'object type':'datasource property' , 
                                                        'datasource name':datasource.get('caption'),
                                                        'property type': 'Filter_' + str(filter_number), 
                                                        'property name': key, 'property value': value,
                                                        'supported in TS': supported_in_ts,
                                                        'supported in Migrator' : supported_in_migrator,
                                                        'conversion in TS': ts_equivalent}])
                                rows.append(row_df)
                            filter_number += 1

                        col = ['object type', 'datasource name', 'property type',   
                                        'property name', 'property value', 'proposed value','conversion in TS', 'supported in TS', 'supported in Migrator']
                        filter_dataframe = pd.DataFrame(columns=col)
                        if rows:
                            filter_dataframe = pd.concat(rows, ignore_index=True)
                        filter_df_list.append(filter_dataframe)
                    
                    # If the filter class is quantitative, process the filters
                    elif filter_class == 'quantitative':
                        
                        # Get min, max values if available
                        min_tag = filter.find('min')
                        max_tag = filter.find('max')
                        min_value = None
                        max_value = None
                        if min_tag:
                            min_value = min_tag.text
                        if max_tag:
                            max_value = max_tag.text
                        quantitative_dict = {}
                        quantitative_dict['class'] = 'quantitative'
                        quantitative_dict['column'] = filter.get('column')
                        quantitative_dict['included_values'] = filter.get('included-values')
                        if min_value and max_value:
                            quantitative_dict['function'] = 'between'
                            quantitative_dict['minimum'] = min_value
                            quantitative_dict['maximum'] = max_value
                        elif min_value:
                            quantitative_dict['function'] = 'atleast'
                            quantitative_dict['minimum'] = min_value
                        elif max_value:
                            quantitative_dict['function'] = 'atmost'
                            quantitative_dict['maximum'] = max_value
                        else :
                            quantitative_dict['function'] = 'undefined'

                        rows = []
                        # Append the values to a dataframe to put them in the dump
                        tableau_class = quantitative_dict['class']
                        func = np.nan
                        if 'function' in quantitative_dict:
                            func = quantitative_dict['function']
                        supported = filter_mapping[
                            (filter_mapping['tableau_class'] == tableau_class) &
                            (filter_mapping['function'] == func)]
                        supported_in_ts =  supported[supported_in_ts_column_name].values[0] if not supported[supported_in_ts_column_name].empty else None
                        supported_in_migrator = supported[supported_in_mig_column_name].values[0] if not supported[supported_in_mig_column_name].empty else None
                        ts_equivalent = supported['datasource filter hint'].values[0] if not supported['datasource filter hint'].empty else None 
                        for key, value in quantitative_dict.items():
                            row_df = pd.DataFrame([{'object type':'datasource property' , 
                                                        'datasource name':datasource.get('caption'),
                                                        'property type': 'Filter_' + str(filter_number), 
                                                        'property name': key, 'property value': value,
                                                        'supported in TS':supported_in_ts, 
                                                        'supported in Migrator':supported_in_migrator,
                                                        'conversion in TS' : ts_equivalent}])
                            rows.append(row_df)
                        filter_number=+1
                        col = ['object type', 'datasource name', 'property type',   
                                        'property name', 'property value', 'proposed value','conversion in TS', 'supported in TS', 'supported in Migrator']
                        filter_dataframe = pd.DataFrame(columns=col)
                        filter_dataframe = pd.concat(rows, ignore_index=True)
                        filter_df_list.append(filter_dataframe)

                    elif filter_class == 'relative-date':
                        first_period = filter.get('first-period')
                        last_period = filter.get('last-period')
                        period_anchor_date = filter.get('period-anchor')
                        period_type = filter.get('period-type-v2')
                        include_null = filter.get('include-null')
                        include_future = filter.get('include-future')
                        
                        if period_anchor_date is None:
                            period_anchor_date = 'today'
                        
                        props = []

                        supported = filter_mapping[
                            (filter_mapping['tableau_class'] == filter_class)]
                        supported_in_ts =  supported[supported_in_ts_column_name].values[0] if not supported[supported_in_ts_column_name].empty else None
                        supported_in_migrator = supported[supported_in_mig_column_name].values[0] if not supported[supported_in_mig_column_name].empty else None
                        ts_equivalent = supported['datasource filter hint'].values[0] if not supported['datasource filter hint'].empty else None 

                        for key, value in {'class' : filter_class, 'column' : filter_column, 'period_type': period_type, 'period_anchor_date': period_anchor_date, 
                                            'first_period': first_period, 'last_period': last_period, 'include_null': include_null, 
                                            'include_future': include_future}.items():
                            prop_df = pd.DataFrame([{'object type':'datasource property' , 
                                                        'datasource name':datasource.get('caption'),
                                                        'property type': 'Filter_' + str(filter_number), 
                                                        'property name': key, 'property value': value,
                                                        'supported in TS':supported_in_ts, 
                                                        'supported in Migrator':supported_in_migrator,
                                                        'conversion in TS' : ts_equivalent}])
                            props.append(prop_df)
                        filter_number += 1

                        col = ['object type', 'datasource name', 'property type',   
                                        'property name', 'property value', 'conversion in TS', 'supported in TS', 'supported in Migrator']
                        filter_dataframe = pd.DataFrame(columns=col)
                        filter_dataframe = pd.concat(props, ignore_index=True)
                        filter_df_list.append(filter_dataframe)
                    
                except Exception as e:
                    get_logger().exception(f"'Filter_{filter_number}' parsing failed : %s", e)
                    error_data = {
                        'twb file name': twb_name,
                        'object type': 'datasource',
                        'exec_error_details': str(e)
                    }

                    df_table = pd.DataFrame([error_data])
                    df_table = df_table.reindex(columns=[
                        'twb file name', 'object type', 'datasource name', 'table name', 
                        'remote column name', 'local column name', 'worksheet name', 
                        'dashboard name', 'property type', 'property name', 'property value',
                        'conversion in TS', 'supported in TS', 'supported in Migrator', 'exec_error_details'])
                    filter_df_list.append(df_table)   

                get_logger().info(f"'Filter_{curr_filter_name}' extraction completed.")   

            get_logger().info(f"Filters extracted from datasource '{datasource.get('caption')}'")  

        ordered_col = ['twb file name', 'object type', 'datasource name', 'table name', 
                    'remote column name', 'local column name', 'worksheet name', 
                    'dashboard name', 'property type',   'property name', 'property value', 
                    'conversion in TS', 'supported in TS', 'supported in Migrator', 'exec_error_details']
        datasource_filter = pd.DataFrame(columns=ordered_col)
        for dataframe in filter_df_list:
            datasource_filter = pd.concat([datasource_filter, dataframe], ignore_index=True)
        col = ['table name','remote column name', 'local column name','worksheet name', 'dashboard name', 'exec_error_details']
        datasource_filter[col] = np.NaN
        datasource_filter['twb file name'] = twb_name
        datasource_filter.reset_index(drop=True, inplace=True)
        datasource_filter = datasource_filter.reindex(columns=ordered_col,)

        get_logger().info(f"Datasource level filters parsing completed for '{twb_name}")

        # Return all the filters
        return datasource_filter

    # Chart Level Filter:
    def chart_level_filter(self, soup, filter_mapping, twb_name, live_flag):

        get_logger().info(f"Parsing chart level filters from '{twb_name}")

        filter_df_list = []
            
        # Find all worksheets available
        worksheets = soup.find('worksheets').find_all('worksheet')

        # Iterate through the worksheets to find the filters present
        for worksheet in worksheets:

            worksheet_name = worksheet.get('name')

            get_logger().info(f"Extracting filters from worksheet '{worksheet_name}'")
            
            # Prepare a datasource dictionary 
            datasources = worksheet.find('datasources').find_all('datasource')
            datasource_dict = {}
            for datasource_tag in datasources:
                datasource_dict[datasource_tag.get('name')] = datasource_tag.get('caption')

            # Find all filters and process the filters
            ws_filters = worksheet.find_all('filter')
            filter_number = 1

            if not ws_filters or len(ws_filters)==0:
                get_logger().info(f"No filters present in the worksheet '{worksheet_name}'")

            for filter in ws_filters:

                curr_filter_name = filter_number

                get_logger().info(f"Processing 'Filter_{curr_filter_name}'")

                try :

                    filter_class = filter.get('class')
                    filter_column_identifier = filter.get('column')

                    datasource_column_split = re.findall(r'\[.*?\]', filter_column_identifier)
                    datasource_identifier = datasource_column_split[0].strip('[]')
                    filter_column = datasource_column_split[1]

                    # Find the datasource name from the column name present in the filter
                    datasource = datasource_dict[datasource_identifier]

                    # This condition only processes categorical filters
                    if filter_class == 'categorical':
                        user_ui_enumeration = None
                        members = []
                        top_dict = {}
                        wildcard_dict = {}
                        condition_dict = {}
                        general_dict = {}
                        isFunctionToExcept = False

                        # Append the first group filter to the queue
                        queue = deque(filter.find_all('groupfilter', recursive=False))

                        # Process the group filters hierarchically
                        while queue:
                            groupfilter = queue.popleft()
                            function = groupfilter.get('function')
                            if function == 'end' :  # if the filter is top, append values to top dictionary
                                top_dict['function'] = 'top'
                                top_dict['count'] = str(groupfilter.get('count'))
                                top_dict['end'] = groupfilter.get('end')
                            elif function == 'order': # if the filter is order, append values to top dictionary
                                top_dict['direction'] = groupfilter.get('direction')
                                top_dict['expression'] = groupfilter.get('expression')
                            elif function == 'filter': # if the filter is filter, append values to wildcard/condition dictionary
                                expression = groupfilter.get('expression')
                                if groupfilter.get('user:ui-pattern_type') or groupfilter.get('user:ui-pattern_text'):
                                    wildcard_dict['expression'] = expression
                                    wildcard_dict['user:ui-enumeration'] = groupfilter.get('user:ui-enumeration')
                                    wildcard_dict['user:ui-pattern_type'] = groupfilter.get('user:ui-pattern_type')
                                    wildcard_dict['user:ui-pattern_text'] = groupfilter.get('user:ui-pattern_text')
                                    wildcard_dict['function'] = 'wildcard'
                                else:
                                    condition_dict['function'] = 'condition'
                                    condition_dict['expression'] = expression
                            # All the other functions belongs to general filter
                            elif function == 'union' and groupfilter.get('user:ui-enumeration') == 'inclusive':
                                user_ui_enumeration = 'inclusive'
                            elif function == 'except' and groupfilter.get('user:ui-enumeration') == 'inclusive':
                                user_ui_enumeration = 'exclusive'
                            elif function == 'except' and groupfilter.get('user:ui-enumeration') == 'exclusive':
                                user_ui_enumeration = 'exclusive'
                            elif function == 'except':
                                isFunctionToExcept = True
                            elif function == 'member' and groupfilter.get('user:ui-enumeration') == 'inclusive':
                                user_ui_enumeration = 'inclusive'
                                members.append(groupfilter.get('member'))
                            elif function == 'member' and groupfilter.get('user:ui-enumeration') == 'exclusive':
                                user_ui_enumeration = 'exclusive'
                                members.append(groupfilter.get('member'))
                            elif function == 'member':
                                members.append(groupfilter.get('member'))
                            elif function == 'empty-level':
                                user_ui_enumeration = groupfilter.get('user:ui-enumeration')
                            elif function == 'range':
                                from_value = groupfilter.get('from')
                                to_value = groupfilter.get('to')
                                for i in range(int(from_value), int(to_value) + 1):
                                    members.append(str(i))
                            
                            # Append the child group filters back to the queue
                            for gf in groupfilter.find_all('groupfilter', recursive=False):
                                queue.append(gf)
                        
                        # If the function is to except everything except few values
                        if isFunctionToExcept is True and user_ui_enumeration == 'exclusive':
                            user_ui_enumeration = 'inclusive'

                        if user_ui_enumeration:
                            general_dict['user:ui-enumeration'] = user_ui_enumeration
                            general_dict['members'] = members

                        # Append the common values to all dictionaries
                        ordered_values = ['class', 'function', 'column',
                                          'user:ui-enumeration','user:ui-pattern_type',
                                          'user:ui-pattern_text','expression','members',
                                          'count','end','direction']
                        if top_dict:
                            top_dict['class'] = 'categorical'
                            top_dict['column'] = filter_column
                            top_dict = {key: top_dict[key] for key in ordered_values if key in top_dict}
                        if general_dict:
                            general_dict['class'] = 'categorical'
                            general_dict['function'] = 'general'
                            general_dict['column'] = filter_column
                            general_dict = {key: general_dict[key] for key in ordered_values if key in general_dict}
                        if condition_dict:
                            condition_dict['class'] = 'categorical'
                            condition_dict['column'] = filter_column
                            condition_dict = {key: condition_dict[key] for key in ordered_values if key in condition_dict}
                        if wildcard_dict:
                            wildcard_dict['class'] = 'categorical'
                            wildcard_dict['column'] = filter_column
                            wildcard_dict = {key: wildcard_dict[key] for key in ordered_values if key in wildcard_dict}

                        rows= []

                        # For general dictionary, get the supported in mode and migrator values 
                        # from the csv and form them as a dataframe to put them in the dump
                        if general_dict:

                            tableau_class = general_dict['class']
                            func = general_dict['function']
                            enumeration = np.nan
                            if 'user:ui-enumeration' in general_dict:
                                enumeration = general_dict['user:ui-enumeration']
                            supported = filter_mapping[
                                (filter_mapping['tableau_class'] == tableau_class) &
                                (filter_mapping['function'] == func) &
                                (filter_mapping['ui-enumeration'].isna() if pd.isna(enumeration) 
                                else filter_mapping['ui-enumeration'] == enumeration)]
                            supported_in_ts =  supported['supported in TS'].values[0] if not supported['supported in TS'].empty else None
                            supported_in_migrator = supported['supported in Live Migrator'].values[0] if not supported['supported in Live Migrator'].empty else None
                            ts_equivalent = supported['chart filter hint'].values[0] if not supported['chart filter hint'].empty else None
                            # Form the dataframes to put them in a dump
                            for key, value in general_dict.items():
                                if key == 'members':
                                    for v in value:
                                        row_df = pd.DataFrame([{'object type':'chart property', 
                                                        'datasource name':datasource,
                                                        'worksheet name': worksheet_name,
                                                        'property type': 'Filter_' + str(filter_number), 
                                                        'property name': 'member', 'property value': v,
                                                        'supported in TS': supported_in_ts,
                                                        'supported in Migrator' : supported_in_migrator,
                                                        'conversion in TS': ts_equivalent}])
                                        rows.append(row_df)
                                else :
                                    row_df = pd.DataFrame([{'object type':'chart property' , 
                                                            'datasource name':datasource,
                                                            'worksheet name': worksheet_name,
                                                            'property type': 'Filter_' + str(filter_number), 
                                                            'property name': key, 'property value': value,
                                                            'supported in TS': supported_in_ts,
                                                            'supported in Migrator' : supported_in_migrator,
                                                            'conversion in TS':ts_equivalent}])
                                    rows.append(row_df)
                            filter_number += 1
                        for d in [d for d in [top_dict, condition_dict, wildcard_dict] if d]:

                            tableau_class = d['class']
                            func = d['function']
                            pattern = np.nan
                            if 'user:ui-pattern_type' in d:
                                pattern = d['user:ui-pattern_type'] 
                            enumeration = np.nan
                            if 'user:ui-enumeration' in d:
                                enumeration = d['user:ui-enumeration']
                            supported = filter_mapping[
                                (filter_mapping['tableau_class'] == tableau_class) &
                                (filter_mapping['function'] == func) &
                                (filter_mapping['pattern-text'].isna() if pd.isna(pattern) 
                                else filter_mapping['pattern-text'] == pattern) &
                                (filter_mapping['ui-enumeration'].isna() if pd.isna(enumeration) 
                                else filter_mapping['ui-enumeration'] == enumeration)]
                            supported_in_ts =  supported['supported in TS'].values[0] if not supported['supported in TS'].empty else None
                            supported_in_migrator = supported['supported in Live Migrator'].values[0] if not supported['supported in Live Migrator'].empty else None
                            ts_equivalent = supported['chart filter hint'].values[0] if not supported['chart filter hint'].empty else None
                            # Append the values to a dataframe to put them in the dump
                            for key, value in d.items():
                                row_df = pd.DataFrame([{'object type':'chart property', 
                                                        'datasource name':datasource,
                                                        'worksheet name': worksheet_name,
                                                        'property type': 'Filter_' + str(filter_number), 
                                                        'property name': key, 'property value': value,
                                                        'supported in TS': supported_in_ts,
                                                        'supported in Migrator' : supported_in_migrator,
                                                        'conversion in TS' : ts_equivalent}])
                                rows.append(row_df)
                            filter_number += 1

                        col = ['object type', 'datasource name', 'property type', 'worksheet name',
                                        'property name', 'property value', 'proposed value','conversion in TS', 'supported in TS', 'supported in Migrator', 'exec_error_details']
                        filter_dataframe = pd.DataFrame(columns=col)
                        if rows:
                            filter_dataframe = pd.concat(rows, ignore_index=True)
                        filter_df_list.append(filter_dataframe)

                    # If the filter class is quantitative, process the filters
                    elif filter_class == 'quantitative':

                        # Find min and max values if available
                        min_tag = filter.find('min')
                        max_tag = filter.find('max')
                        min_value = None
                        max_value = None
                        if min_tag:
                            min_value = min_tag.text
                        if max_tag:
                            max_value = max_tag.text
                        quantitative_dict = {}
                        quantitative_dict['class'] = 'quantitative'
                        quantitative_dict['column'] = filter.get('column')
                        quantitative_dict['included_values'] = filter.get('included-values')
                        if min_value and max_value:
                            quantitative_dict['function'] = 'between'
                            quantitative_dict['minimum'] = min_value
                            quantitative_dict['maximum'] = max_value
                        elif min_value:
                            quantitative_dict['function'] = 'atleast'
                            quantitative_dict['minimum'] = min_value
                        elif max_value:
                            quantitative_dict['function'] = 'atmost'
                            quantitative_dict['maximum'] = max_value
                        else :
                            quantitative_dict['function'] = 'undefined'
    
                        rows = []
                        tableau_class = quantitative_dict['class']
                        func = np.nan
                        if 'function' in quantitative_dict:
                            func = quantitative_dict['function']
                        supported = filter_mapping[
                            (filter_mapping['tableau_class'] == tableau_class) &
                            (filter_mapping['function'] == func)]
                        supported_in_ts =  supported['supported in TS'].values[0] if not supported['supported in TS'].empty else None
                        supported_in_migrator = supported['supported in Live Migrator'].values[0] if not supported['supported in Live Migrator'].empty else None              
                        ts_equivalent = supported['chart filter hint'].values[0] if not supported['chart filter hint'].empty else None
                        for key, value in quantitative_dict.items():
                            row_df = pd.DataFrame([{'object type':'chart property' , 
                                                        'datasource name':datasource,
                                                        'worksheet name': worksheet_name,
                                                        'property type': 'Filter_' + str(filter_number), 
                                                        'property name': key, 'property value': value,
                                                        'supported in TS':supported_in_ts, 
                                                        'supported in Migrator':supported_in_migrator,
                                                        'conversion in TS' : ts_equivalent}])
                            rows.append(row_df)
                        filter_number=+1
                        col = ['object type', 'datasource name', 'worksheet name', 'property type', 
                                        'property name', 'property value', 'conversion in TS', 'supported in TS', 'supported in Migrator','exec_error_details']
                        filter_dataframe = pd.DataFrame(columns=col)
                        filter_dataframe = pd.concat(rows, ignore_index=True)
                        filter_df_list.append(filter_dataframe)

                    elif filter_class == 'relative-date':
                        first_period = filter.get('first-period')
                        last_period = filter.get('last-period')
                        period_anchor_date = filter.get('period-anchor')
                        period_type = filter.get('period-type-v2')
                        include_null = filter.get('include-null')
                        include_future = filter.get('include-future')
                        
                        if period_anchor_date is None:
                            period_anchor_date = 'today'
                        
                        props = []

                        supported = filter_mapping[
                            (filter_mapping['tableau_class'] == filter_class)]
                        supported_in_ts =  supported['supported in TS'].values[0] if not supported['supported in TS'].empty else None
                        supported_in_migrator = supported['supported in Live Migrator'].values[0] if not supported['supported in Live Migrator'].empty else None
                        ts_equivalent = supported['datasource filter hint'].values[0] if not supported['datasource filter hint'].empty else None 

                        for key, value in {'class' : filter_class, 'column' : filter_column, 'period_type': period_type, 'period_anchor_date': period_anchor_date, 
                                            'first_period': first_period, 'last_period': last_period, 'include_null': include_null, 
                                            'include_future': include_future}.items():
                            prop_df = pd.DataFrame([{'object type':'datasource property' , 
                                                        'datasource name':datasource.get('caption'),
                                                        'property type': 'Filter_' + str(filter_number), 
                                                        'property name': key, 'property value': value,
                                                        'supported in TS':supported_in_ts, 
                                                        'supported in Migrator':supported_in_migrator,
                                                        'conversion in TS' : ts_equivalent}])
                            props.append(prop_df)
                        filter_number += 1

                        col = ['object type', 'datasource name', 'property type',   
                                        'property name', 'property value', 'conversion in TS', 'supported in TS', 'supported in Migrator']
                        filter_dataframe = pd.DataFrame(columns=col)
                        filter_dataframe = pd.concat(props, ignore_index=True)
                        filter_df_list.append(filter_dataframe)
                    
                except Exception as e:
                    error_data = {
                        'twb file name': twb_name,
                        'object type': 'chart',
                        'exec_error_details': str(e)
                    }

                    df_table = pd.DataFrame([error_data])
                    df_table = df_table.reindex(columns=[
                        'twb file name', 'object type', 'datasource name', 'table name', 
                        'remote column name', 'local column name', 'worksheet name', 
                        'dashboard name', 'property type', 'property name', 'property value',
                        'conversion in TS', 'supported in TS', 'supported in Migrator', 'exec_error_details'])
                    filter_df_list.append(df_table)

                get_logger().info(f"'Filter_{curr_filter_name}' extraction completed.")
            
            get_logger().info(f"Filters extracted from worksheet '{worksheet_name}'") 

        ordered_col = ['twb file name', 'object type', 'datasource name',  'worksheet name', 'table name',  
                    'remote column name', 'local column name', 'dashboard name', 'property type',    'property name', 'property value', 
                    'conversion in TS', 'supported in TS', 'supported in Migrator', 'exec_error_details']
        chart_filter = pd.DataFrame(columns=ordered_col)
        for dataframe in filter_df_list:
            chart_filter = pd.concat([chart_filter, dataframe], ignore_index=True)
        col = ['table name','remote column name', 'local column name','dashboard name', 'exec_error_details']
        chart_filter[col] = np.NaN
        chart_filter['twb file name'] = twb_name
        chart_filter.reset_index(drop=True, inplace=True)
        chart_filter = chart_filter.reindex(columns=ordered_col,)

        get_logger().info(f"Chart level filters parsing completed for '{twb_name}")

        # Return all the filters
        return chart_filter
