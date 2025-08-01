import pandas as pd
import os
import numpy as np
import re
from utilities import file_reader
from utilities.logger_config import get_logger

class Filter_Migrator:
        
        def __init__(self):
            # sql filters mapping csv file
            self.sql_csv_mapping = file_reader.get_mapping_file()['sql_filter_mapping.csv']
            self.date_filters = file_reader.get_mapping_file()['date_filters.csv']

        def form_filter_queries(self, filters_df, table_column_relationship_map, datasource):

            get_logger().info("Generating extract filter queries")
            # Get all the unique filters present
            filter_names = filters_df['property type'].unique().tolist()
            where_queries = []
            cte_queries = []
            cte_joins = []
            date_filters = self.date_filters
            sql_csv_mapping = self.sql_csv_mapping

            # Iterate through each filter name to process the filters
            for filter in filter_names:

                get_logger().info(f"Generating filter SQL query for '{filter}'")

                try :
                    # Retrieving the respective filter details
                    filter_details = filters_df[filters_df['property type']==filter]

                    # Find the column name
                    column_name =  filter_details.loc[filter_details['property name'] == 'column', 'property value'].values[0]
                    aggregation = np.nan
                    if column_name is not None:
                        if match := re.match(r'\[([^:]*):([^:]*):[^:]*\]', column_name):
                            if match.group(1) != 'none':
                                aggregation = match.group(1) 
                            column_name = match.group(2)
                        column_name=column_name.replace('[','').replace(']','')
                    
                    if column_name.startswith('Calculation'):
                        self.raise_exception(f"Filter applied column '{column_name}' is a calculated column. Query generation failed for '{filter}' in '{datasource}' as this case not supported as of now.")

                    # Getting the filter's class
                    filter_class = filter_details.loc[filter_details['property name'] == 'class', 'property value']
                    filter_class = filter_class.values[0] if not filter_class.empty else np.nan

                    # Getting the filter's function
                    function = filter_details.loc[filter_details['property name'] == 'function', 'property value']
                    function = function.values[0] if not function.empty else np.nan

                    # Getting the end value (end represents top/bottom)
                    end = filter_details.loc[filter_details['property name'] == 'end', 'property value']
                    end = end.values[0] if not end.empty else np.nan

                    # Getting user ui enumeration (inclusive/exclusive)
                    ui_enumeration = filter_details.loc[filter_details['property name'] == 'user:ui-enumeration', 'property value']
                    ui_enumeration = ui_enumeration.values[0] if not ui_enumeration.empty else np.nan

                    # Getting ui pattern type (contains/starts-with/ends-with/exact-match)
                    ui_pattern_type = filter_details.loc[filter_details['property name'] == 'user:ui-pattern_type', 'property value']
                    ui_pattern_type = ui_pattern_type.values[0] if not ui_pattern_type.empty else np.nan

                    # Getting value of ui_pattern_filter_empty
                    ui_pattern_filter_empty = filter_details.loc[filter_details['property name'] == 'user:ui-pattern-filter-empty', 'property value']
                    ui_pattern_filter_empty = ui_pattern_filter_empty.values[0] if not ui_pattern_filter_empty.empty else np.nan

                    # Getting value of ui_pattern_include_all_when_empty
                    ui_pattern_include_all_when_empty = filter_details.loc[filter_details['property name'] == 'user:ui-pattern-include-all-when-empty','property value']
                    ui_pattern_include_all_when_empty = ui_pattern_include_all_when_empty.values[0] if not ui_pattern_include_all_when_empty.empty else np.nan
                    
                    # Getting all the member values and adding it to the list
                    members = filter_details.loc[filter_details['property name'] == 'member', 'property value'].tolist() 
                    members = members if members else None

                    # Decides whether the null values needs to be included in the members list
                    include_null = np.nan
                    if members is not None and "%null%" in members:
                        members.remove("%null%")
                        include_null = True
                    elif members is not None and "%null%" not in members:
                        include_null = False

                    # Query Template Identification
                    filtered_filter_details = sql_csv_mapping[(sql_csv_mapping['class'] == filter_class) 
                                                    & (sql_csv_mapping['include-null'].isna() if pd.isna(include_null)  
                                                        else sql_csv_mapping['include-null' ] == include_null) 
                                                    & (sql_csv_mapping['function'].isna() if pd.isna(function) 
                                                        else sql_csv_mapping['function'] == function)
                                                    & (sql_csv_mapping['end'].isna() if pd.isna(end) 
                                                        else sql_csv_mapping['end'] == end) 
                                                    & (sql_csv_mapping['user:ui-enumeration'].isna() if pd.isna(ui_enumeration) 
                                                        else sql_csv_mapping['user:ui-enumeration'] == ui_enumeration) 
                                                    & (sql_csv_mapping['user:ui-pattern_type'].isna() if pd.isna(ui_pattern_type)
                                                        else sql_csv_mapping['user:ui-pattern_type'] == ui_pattern_type) 
                                                    & (sql_csv_mapping['user:ui-pattern-filter-empty'].isna() if pd.isna(ui_pattern_filter_empty)
                                                        else sql_csv_mapping['user:ui-pattern-filter-empty'] == ui_pattern_filter_empty) 
                                                    & (sql_csv_mapping['user:ui-pattern-include-all-when-empty'].isna() if pd.isna(ui_pattern_include_all_when_empty) 
                                                        else sql_csv_mapping['user:ui-pattern-include-all-when-empty'] == ui_pattern_include_all_when_empty) 
                                                    & (sql_csv_mapping['aggregation'].isna() if pd.isna(aggregation)
                                                       else sql_csv_mapping['aggregation']==aggregation)
                                                ]
                    template = None
                    if not filtered_filter_details.empty:
                        template = filtered_filter_details['query-template'].values[0]
                    
                    if not template or template is None:
                        if filter_class != 'categorical' and filter_class != 'quantitative':
                            self.raise_exception(f"'{filter_class}' filters migration not supported as of now. Query generation failed for '{filter}' present in '{datasource}'.")
                        self.raise_exception(f"No valid template found for '{filter}' present in '{datasource}'. Query generation failed.")
                    
                    get_logger().info(f"Filter Query Template: '{template}'")

                    # Find maximum and minimum values if exists (in case of quantitative filters)
                    minimum = filter_details.loc[filter_details['property name'] == 'minimum', 'property value']
                    minimum = minimum.values[0] if not minimum.empty else None

                    maximum = filter_details.loc[filter_details['property name'] == 'maximum', 'property value']
                    maximum = maximum.values[0] if not maximum.empty else None

                    # If the value is Date refactor the format of the date
                    if minimum is not None and (match := re.match(r'^#(\d{4}-\d{2}-\d{2})#$', minimum)):
                            minimum = "'"+match.group(1)+"'"
                    if maximum is not None and (match := re.match(r'^#(\d{4}-\d{2}-\d{2})#$', maximum)):
                            maximum = "'"+match.group(1)+"'"
                    
                    # Find count in case of top filter
                    count = filter_details.loc[filter_details['property name'] == 'count', 'property value']
                    count = count.values[0] if not count.empty else None

                    # Find pattern_text in case of wild card filter
                    pattern_text = filter_details.loc[filter_details['property name'] == 'user:ui-pattern_text', 'property value']
                    pattern_text = pattern_text.values[0] if not pattern_text.empty else None

                    # Find direction incase of top filter
                    direction = filter_details.loc[filter_details['property name'] == 'direction', 'property value']
                    direction = direction.values[0] if not direction.empty else None

                    # Find expression incase of condition/wildcard filters
                    expression = filter_details.loc[filter_details['property name'] == 'expression', 'property value']
                    expression = expression.values[0].replace('[','').replace(']','') if not expression.empty else None
                        
                    if expression is not None and 'Calculation' in expression:
                        self.raise_exception(f"'{filter}' contains calculated column reference. Query generation failed as this case not supported as of now.")
                                                   
                    # Create the filter query from the template
                    member = None
                    if members:
                        if aggregation == 'my':
                            member = str([(date[:4], str(int(date[4:]))) for date in members])
                        elif aggregation == 'md':
                            member = str([(date_str[:4], str(int(date_str[4:6])), date_str[6:]) for date_str in members])
                        else:
                            member = str(members)
                        member = member.replace('[', '(').replace(']',')').replace('"','').replace('#','')

                    filter_query = template.format(column=column_name,
                                    min=minimum,
                                    max=maximum,
                                    count=count,
                                    pattern_text=pattern_text,
                                    direction=direction,
                                    expression=expression,
                                    member=member,
                                    table=table_column_relationship_map[column_name])
                
                    if filter_class == 'quantitative':
                        where_queries.append(filter_query)
                    elif filter_class == 'categorical':
                        if function == 'general':
                            where_queries.append(filter_query)
                        else:
                            join_column_name = f'{table_column_relationship_map[column_name]}.{column_name}'
                            if not pd.isna(aggregation):
                                if aggregation == 'my':
                                    jq = f"\nINNER JOIN {filter} ON YEAR({table_column_relationship_map[column_name]}.{column_name}) = {filter}.year " \
                                            f"AND MONTH({table_column_relationship_map[column_name]}.{column_name}) = {filter}.month"
                                    cte_joins.append(jq)
                                elif aggregation == 'md':
                                    jq = f"\nINNER JOIN {filter} ON YEAR({table_column_relationship_map[column_name]}.{column_name}) = {filter}.year " \
                                            f"AND MONTH({table_column_relationship_map[column_name]}.{column_name}) = {filter}.month " \
                                            f"AND DAY({table_column_relationship_map[column_name]}.{column_name}) = {filter}.day"
                                    cte_joins.append(jq)
                                else:
                                    row = date_filters.loc[date_filters['aggregation'] == aggregation, 'join_column_name']
                                    if not row.empty and not pd.isna(row.values[0]):
                                        join_column_name = row.values[0].format(
                                            table_name=table_column_relationship_map[column_name],
                                            column_name=column_name
                                        )
                                    column_name = date_filters[(date_filters['aggregation'] == aggregation)]['column_name'].values[0]
                                    jq = f"\nINNER JOIN {filter} ON {join_column_name} = {filter}.{column_name}"
                                    cte_joins.append(jq)
                            else :
                                jq = f"\nINNER JOIN {filter} ON {join_column_name} = {filter}.{column_name}"
                                cte_joins.append(jq)
                            cte_queries.append(f"{filter} as ({filter_query})")

                except Exception as e:
                     get_logger().exception(f"'{filter}' generation failed : %s", e)

            # form filter join query
            join_query = ''
            for jq in cte_joins:
                join_query += jq

            # form filter where queries
            where_query = ''
            if where_queries:
                where_query = "\nWHERE " +  "\nAND ".join(where_queries)  

            # form all the filter queries
            cte_query = ''
            if cte_queries:
                cte_query = ',\n'.join(cte_queries)
                cte_query = cte_query + '\n'
            
            get_logger().info("Filter queries creation completed")
            return join_query, where_query, cte_query
        

        def raise_exception(self, exception_message):
            raise ValueError(exception_message)