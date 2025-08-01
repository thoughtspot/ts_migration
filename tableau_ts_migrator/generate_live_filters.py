from datetime import datetime
import csv
import pandas as pd
from utilities import file_reader
from utilities.logger_config import get_logger

class LiveDataFilter:
    "Class for live filter"

    def __init__(self) -> None:
        pass

    def live_filter(self,filters_df,table_column_relationship_map, datasource):
        "Function for live filter"
        csv_data = file_reader.get_mapping_file()['live_filters.csv']
        filters_name_list = filters_df['property type'].unique().tolist()
        filter_results = []
        for filter in filters_name_list:

            get_logger().info(f"Processing Live Filter with the name '{filter}'")

            try:
                filter_df = filters_df[filters_df['property type']==filter]
                filter_result = self.process_filters(filter_df, csv_data,table_column_relationship_map,datasource,filter)
                filter_results.append(filter_result)
                get_logger().info(f"'{filter}' generation completed")
            except Exception as e:
                get_logger().exception(f"'{filter}' generation failed : %s", e)
                
        return filter_results

    def standardize_column_format(self, column_value):
        "Function for standardizing column to the same format"
        if column_value.startswith('[') and column_value.endswith(']'):
            parts = column_value.strip('[]').split(':')
            return parts[-2] if len(parts) > 1 else parts[-1]
        return column_value


    def load_csv_data(self, csv_file_path):
        "Load and read CSV"
        with open(csv_file_path, mode='r', newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)
            csv_data = [row for row in csv_reader]
        return csv_data

    def format_date(self, date_str):
        "Format date to the same format"
        if date_str.startswith('#') and date_str.endswith('#'):
            date_str = date_str.strip('#')
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            formatted_date_str = date_obj.strftime('%m/%d/%Y')
            return formatted_date_str
        return date_str

    def raise_exception(self, exception_message):
        raise ValueError(exception_message)

    def process_filters(self, filters_df, csv_data, table_column_relationship_map, datasource, filter_name):
        """Process filter details"""
        filter_result = []
        def is_date(string):
            """Check if the string is a valid date."""
            try:
                datetime.strptime(string, '%Y-%m-%d')
                return True
            except ValueError:
                return False

        def format_value(value):
            """Format value as string with specific rules."""
            if isinstance(value, str):
                if value.startswith('#') and value.endswith('#'):
                    value = self.format_date(value)
                elif is_date(value):
                    value = self.format_date(value)
            return value

        def clean_column_name(column_name: str) -> str:
            # Remove leading/trailing spaces and brackets
            column_name = column_name.strip('[]').strip()

            # Further clean up specific patterns
            if ':' in column_name:
                # Split by colon and return the middle value if it exists
                parts = column_name.split(':')
                if len(parts) >= 3:
                    return parts[1]
            return column_name

        # Initialize variables to None
        class_value = None
        column_value = None
        included_values = None
        min_value = None
        max_value = None
        ui_enumeration = None
        function_type = None
        ui_pattern_type = None
        ui_pattern_text = None

        # Set variables based on conditions
        for _, row in filters_df.iterrows():
            if row['property name'] == 'class':
                class_value = row['property value']
            elif row['property name'] == 'column':
                column_value = clean_column_name(row['property value'])
            elif row['property name'] == 'included-values':
                included_values = row['property value']
            elif row['property name'] == 'minimum':
                min_value = row['property value']
            elif row['property name'] == 'maximum':
                max_value = row['property value']
            elif row['property name'] == 'user:ui-enumeration':
                ui_enumeration = row['property value']
            elif row['property name'] == 'function':
                function_type = row['property value'].strip()
            elif row['property name'] == 'user:ui-pattern_type':
                ui_pattern_type = row['property value']
            elif row['property name'] == 'user:ui-pattern_text':
                ui_pattern_text = row['property value']

            filter_data = {
                'column': [column_value] if column_value else [],
                'oper': None,
                'values': []
            }
        
        if class_value != 'categorical' and class_value != 'quantitative':
                self.raise_exception(f"'{class_value}' filters migration not supported as of now. Query generation failed for '{filter_name}' present in '{datasource}'.")


        filtered_filter_details = csv_data[(csv_data['class_value'] == class_value)
                                                & (csv_data['included_values'].isna() if pd.isna(included_values)
                                                    else csv_data['included_values'] == included_values)
                                                & (csv_data['function_type'].isna() if pd.isna(function_type)
                                                    else csv_data['function_type'] == function_type)
                                                & (csv_data['ui_enumeration'].isna() if pd.isna(ui_enumeration)
                                                    else csv_data['ui_enumeration'] == ui_enumeration)
                                                & (csv_data['ui_pattern_type'].isna() if pd.isna(ui_pattern_type)
                                                   else csv_data['ui_pattern_type'] == ui_pattern_type)
                                               ]
        condition = None
        if not filtered_filter_details.empty:
            condition = filtered_filter_details['condition'].values[0]
            if class_value == 'categorical':
                if function_type == 'general':
                    get_logger().info(f"The filter is general")
                    member_values = filters_df[filters_df['property name'] == 'member']['property value']
                    values = []
                    for value in member_values:
                        if value:
                            values.append(value)
                            filter_data['values'] = values
                    if values:
                        filter_result.append({
                            'column': [column_value],
                            'oper': condition,
                            'values': filter_data['values']
                        })
                        return filter_result
                elif function_type == 'wildcard':
                    get_logger().info("The filter is wildcard")
                    formula_value = ui_pattern_text
                    value_len = len(formula_value)
                    table_name = table_column_relationship_map[column_value]
                    expr = None
                    formula_result = []
                    if ui_pattern_type == 'exact-match' and ui_enumeration == 'inclusive':
                        filter_result.append({
                        'column': [column_value],
                        'oper': condition,
                        'values': [formula_value]
                        })
                        return filter_result
                    if ui_pattern_type == 'exact-match' and ui_enumeration == 'exclusive':
                        filter_result.append({
                        'column': [column_value],
                        'oper': condition,
                        'values': [formula_value]
                        })
                        return filter_result
                    if table_name:
                        if ui_pattern_type == 'ends-with' and ui_enumeration == 'inclusive':
                            expr = f"right ([{table_name}::{column_value}] , {value_len})"
                        elif ui_pattern_type == 'ends-with' and ui_enumeration == 'exclusive':
                            expr = f"right ([{table_name}::{column_value}] , {value_len})"
                        elif ui_pattern_type == 'starts-with' and ui_enumeration=='inclusive':
                            expr = f"left ([{table_name}::{column_value}] , {value_len})"
                        elif ui_pattern_type == 'starts-with' and ui_enumeration == 'exclusive':
                            expr = f"left ([{table_name}::{column_value}] , {value_len})"

                        formula_name = "formula1"
                        formula_result.append({
                            'name': formula_name,
                            'expr': expr,
                            'was_auto_generated': False
                        })
                        if formula_value:
                            filter_result.append({
                                'formulas': formula_result,
                                'column': [formula_name],
                                'oper': condition,
                                'values': formula_value
                            })
                        return filter_result

        if class_value == 'quantitative':
            get_logger().info("The filter is quantitative")
            min_value = format_value(min_value) if min_value is not None else None
            max_value = format_value(max_value) if max_value is not None else None

            for _, row in csv_data.iterrows():
                if class_value == row['class_value']:
                    if function_type == row['function_type']:
                        operation_to_compare = '> {min_value}'
                        if row['operation'].strip() == operation_to_compare and min_value is not None:
                            oper = row['condition']
                            filter_data['values'] = [f"{min_value}"]
                        elif row['identifier'] == 'max':
                            operation_to_compare = '< {max_value}'
                            if row['operation'].strip() == operation_to_compare and max_value is not None:
                                oper = row['condition']
                                filter_data['values'] = [f"{max_value}"]
                        elif row['identifier'] == 'min&max':
                            operation_to_compare = 'between{range_value}'
                            if row['operation'].strip() == operation_to_compare and  min_value is not None and max_value is not None:
                                oper = row['condition']
                                filter_data['values'] = [min_value, max_value]
                        if filter_data['values']:
                            filter_result.append({
                                'column': [column_value],
                                'oper': oper,
                                'values': filter_data['values']
                            })
                        return filter_result
                    if row['included_values'] == 'non-null':
                        operation_to_compare = 'not in {null}'
                        if row['operation'].strip() == operation_to_compare:
                            oper = row['condition']
                            filter_data['values'] = ['{null}']
                        if filter_data['values']:
                            filter_result.append({
                                'column': [column_value],
                                'oper': oper,
                                'values': filter_data['values']
                            })
                        return filter_result