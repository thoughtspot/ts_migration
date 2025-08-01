#Import necessary modules and functions
# from datasource import *
from .table import *
from .datatype import *
import re
from utilities.formula_conversion import formula
from utilities.logger_config import get_logger
import traceback
class Column():

    def __init__(self):
        pass

    #Function to generate feasibility report for column
    def find_column(self, soup,twb_name):
        try:
            get_logger().info(f'Column parsing started for {twb_name} file')
            data_dict = []

            for datasource in soup.find('datasources').find_all('datasource'):
                data = datasource.find('metadata-records')
                for record in data.find_all('metadata-record'):
                    if record['class'] == 'column':
                        data_dict.append({
                            'object type': record['class'],
                            'datasource name':datasource['caption'],
                            'parent-name': record.find('parent-name').text.strip('[]'), # table name
                            'local-name': record.find('local-name').text.strip('[]'), # local column name
                            'remote-name': record.find('remote-name').text.strip('[]'), # remote column name
                            'local-type': record.find('local-type').text,
                        })
                get_logger().info(f'Processing column properties for {datasource["caption"]} datasource')

            df_column = pd.DataFrame.from_dict(data_dict)
            df_column.rename(columns = {'parent-name':'table name', 'remote-name' : 'remote column name',
                                                'local-name':'local column name','local-type':'property value'}, inplace = True)
            df_column['property type'] = 'metadata-record'
            df_column['property name'] = 'data type'
            df_column['supported in TS']= ''
            df_column['supported in Migrator']= ''
            
            ordered_col = ['twb file name', 'object type', 'datasource name', 'table name',	'remote column name', 'local column name',
       'worksheet name', 'dashboard name', 'property type',	'property name', 'property value', 'proposed value',
       'conversion in TS', 'supported in TS', 'supported in Migrator', 'exec_error_details']
            
            col = ['worksheet name', 'dashboard name', 'conversion in TS',
       'supported in TS', 'supported in Migrator','exec_error_details', 'proposed value']
            df_column[col] = np.NaN
            df_column['twb file name'] = twb_name
            df_column.reset_index(drop=True, inplace=True)
            
            df_column = df_column.reindex(columns=ordered_col,)
            get_logger().info(f'Column parsing completed for {twb_name} file')
            return df_column
        
        except Exception as e:
            get_logger().exception(f'Error in find_column: {e}')
            error_data = {
                'twb file name': twb_name,
                'object type': 'column',
                'exec_error_details': str(e)
            }

            df_column = pd.DataFrame([error_data])
            df_column = df_column.reindex(columns=[
                'twb file name', 'object type', 'datasource name', 'table name', 
                'remote column name', 'local column name', 'worksheet name', 'dashboard name', 'property type', 
                'property name', 'property value', 'proposed value','conversion in TS', 
                'supported in TS', 'supported in Migrator', 'exec_error_details'
            ])

            return df_column

    #Function to generate feasibility report for calculated_column
    def find_calculated(self, soup, twb_name):
        try:
            get_logger().info(f'Calculated column parsing started for {twb_name} file')
            formula_dict = {}
            data_dict = []
            for datasource in soup.find('datasources').find_all('datasource'):
                data = datasource.find_all('column')
                for col in data:
                    if col.find ('calculation') is not None:
                        # if caption is not present, then use name
                        try:
                            name = col['caption']
                            # col['caption'] if col['caption'] is not None else col['name']
                        except KeyError as e:
                            if str(e) == "'caption'":
                                name = col['name']
                                get_logger().info(f"KeyError: 'caption' key not found for column {col['name']}")
                            else:
                                get_logger().error(f"KeyError: {e}")
                        formula_dict[col['name']] = name
                        formula_twb, formula_tml = extract_formula(col.find('calculation')['formula'],formula_dict)
                        data_dict.append(({
                            'object type': 'column',
                            'datasource name':datasource['caption'],
                            'table name': '',
                            'local column name': name,
                            'property type': 'calculated_column',
                            'property name': col['name'],
                            'property value': '(' + formula_twb + ')',
                            'conversion in TS': formula_tml,
                            'supported in TS': str(is_supported_in_ts(formula_tml)),
                            'supported in Migrator': str(is_supported_in_migrator(formula_tml))
                        }))
                get_logger().info(f'Processing calculated column properties for {datasource["caption"]} datasource')

            df_calculated = pd.DataFrame.from_dict(data_dict)
            df_calculated['property type'] = 'calculated_column'
            # df_calculated['property name'] = 'data type'
            # df_calculated['supported in TS']= ''
            # df_calculated['supported in Migrator']= ''
            ordered_col = ['twb file name', 'object type', 'datasource name', 'table name',	'remote column name', 'local column name',
       'worksheet name', 'dashboard name', 'property type',	'property name', 'property value', 'proposed value',
       'conversion in TS', 'supported in TS', 'supported in Migrator', 'exec_error_details']
            col = ['remote column name', 'worksheet name', 'dashboard name', 'supported in TS', 'supported in Migrator', 'exec_error_details', 'proposed value']
            df_calculated[col] = np.NaN
            df_calculated['twb file name'] = twb_name
            df_calculated.reset_index(drop=True, inplace=True)
            df_calculated = df_calculated.reindex(columns=ordered_col,)
            get_logger().info(f'Calculated column parsing completed for {twb_name} file')
            return df_calculated

        except Exception as e:
            get_logger().exception(f'Error in find_column: {e}')
            error_data = {
                'twb file name': twb_name,
                'object type': 'column',
                'exec_error_details': str(e),
                'property type': 'calculated_column'
            }

            df_calculated = pd.DataFrame([error_data])
            df_calculated = df_calculated.reindex(columns=[
                'twb file name', 'object type', 'datasource name', 'table name', 
                'remote column name', 'local column name', 'worksheet name', 'dashboard name', 'property type', 
                'property name', 'property value', 'proposed value', 'conversion in TS', 
                'supported in TS', 'supported in Migrator', 'exec_error_details'
            ])

            return df_calculated
            

def extract_formula(text, replacement_dict):
    # Function to match and replace with dictionary values
    def replacement(match):
        key = match.group(0) # Remove the square brackets
        return f'[{replacement_dict.get(key, key)}]'  # Replace with dictionary value, or keep original

    # Use regex to find all instances of 'Calculation_<number>'
    twb_formula =  re.sub(r'\[Calculation_\d+\]', replacement, text)
    tml_formula = ''
    # to be removed later
    try:
        tml_formula = formula.convert(twb_formula)
    except Exception as e:
        tml_formula = "error"
        print("Error in conversion", e, twb_formula)
    #print("twb_formula", twb_formula, "tml_formula", tml_formula)
    return twb_formula, tml_formula

def is_supported_in_migrator(tml_formula):
    if 'TBD' in tml_formula:
        return 'Partial'
    if 'None' in tml_formula:
        return 'NoSup'
    else:
        return 'Full'

def is_supported_in_ts(tml_formula):
    if 'TBD' in tml_formula:
        return 'Partial'
    if 'None' in tml_formula:
        return 'Default'
    else:
        return 'Full'