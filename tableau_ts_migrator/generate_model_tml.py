# pylint: disable=too-many-locals, too-many-statements, line-too-long, consider-using-enumerate, too-many-branches
'''Importing the necessary modules'''
import os
import json
import re
import pandas as pd
from thoughtspot_tml.utils import determine_tml_type
from utilities import file_reader
from thoughtspot_tml import Model
from utilities.logger_config import get_logger
# from dotenv import load_dotenv

class ModelGenerate:
    '''Class to generate model TML'''

    def __init__(self) -> None:
        self.join_map = file_reader.get_mapping_file()['joins.csv']
        self.model_tml_template = self.load_template("TEMPLATE.model.tml")
    
    def load_template(self, file_name):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_in_a_path = os.path.join(current_dir, 'tml_templates', file_name)
        table_tml_directory = determine_tml_type(path=file_in_a_path)
        return table_tml_directory.load(path=file_in_a_path)

    def return_table_list(self, join_df):
        '''Function to return the list of tables that contains joins'''
        model_tab = list(join_df['Source'].unique())
        tables = []

        # Check if 'Destination' column exists in join_df
        if 'Destination' in join_df.columns:
            for i in model_tab:
                df = join_df[join_df['Source'] == i]
                df.reset_index(drop=True, inplace=True)
                t_name = [i]
                for j in df['Destination']:
                    t_name.append(j)
                tables.append(t_name)
        else:
            # If no joins, only source tables are added to the list
            for i in model_tab:
                tables.append([i])
        return tables

    def create_join_df(self, tables_list, data_df, join_df, datasource):
        '''Function to create dataframe that contains necessary tables as well as its joins details'''

        table_name = []
        column_name = []
        datatype = []
        columntype = []
        aggregation_type = []
        join_type = []
        destination = []
        joinname = []
        model_names = []
        joinvalue = []
        for i in range(len(tables_list)):
            tablename = tables_list[i]
            table_name.append(tablename)
            df_new = data_df[data_df['Table Name'] == tablename]
            df_new.reset_index(drop=True, inplace=True)
            c_name = []
            dt_name = []
            cl_type = []
            agg_name = []
            for j in range(df_new.shape[0]):
                c_name.append(df_new['Remote Column Name'][j])
                dt_name.append(df_new['Data Type'][j])
                cl_type.append(df_new['Column Type'][j])
                agg_name.append(df_new['Aggregation Type'][j])
            column_name.append(c_name)
            datatype.append(dt_name)
            columntype.append(cl_type)
            aggregation_type.append(agg_name)

            jtype = []
            d_table = []
            jname = []
            jval = []

            if tablename in list(join_df['Source'].unique()):
                df_join = join_df[join_df['Source'] == tablename]
                df_join.reset_index(drop=True, inplace=True)

                has_join_type = 'Join Type' in df_join.columns
                has_destination = 'Destination' in df_join.columns
                has_join_name = 'Join Name' in df_join.columns
                has_join_value = 'Join Value' in df_join.columns

                for k in range(df_join.shape[0]):
                    jtype.append(df_join['Join Type'][k] if has_join_type else 'NA')
                    d_table.append(df_join['Destination'][k] if has_destination else 'NA')
                    jname.append(df_join['Join Name'][k] if has_join_name else 'NA')
                    jval.append(df_join['Join Value'][k] if has_join_value else 'NA')

            join_type.append(jtype)
            destination.append(d_table)
            joinname.append(jname)
            model_names.append(datasource)
            joinvalue.append(jval)

        # Create DataFrame from the collected data
        model_df = pd.DataFrame({
            'Table Name': table_name,
            'Column Name': column_name,
            'Data Type': datatype,
            'Column Type': columntype,
            'Aggregation': aggregation_type,
            'Join Type': join_type,
            'Destination': destination,
            'Join Name': joinname,
            'Model Name': model_names,
            'Join Value': joinvalue
        })
        return model_df

    def create_model_tml(self, model_df, formula_details_df, datasource, output_path, filter_value_list):
        '''Function to generate Worksheet TMLs'''
        get_logger().info(f"Model TML creation started for the Live datasource : '{datasource}'")

        try :

            join_mapping_df = self.join_map
            join_mapping_dict = dict(zip(join_mapping_df['Tableau Join'].str.upper(), join_mapping_df['TS Join'].str.upper()))
            mltml = self.model_tml_template.to_dict()
            mltml['guid'] = ' '

            if not model_df.empty:
                mltml['model']['name'] = model_df['Model Name'][0]

                tables = set(model_df['Table Name'].dropna().unique())
                destinations = set(model_df['Destination'].explode().dropna().unique())
                all_tables = tables.union(destinations)

                tab = [{'name': table} for table in all_tables]
                mltml['model']['model_tables'] = tab

                def string_concat(name1, name2):
                    return name1.upper() + '::' + name2

                column_vals = []
                unique_cols = []
                formula_vals = self.append_formulas(formula_details_df)

                formula_filter_results = []
                non_formula_filter_results = []

                if filter_value_list is not None:
                    for filter_value in filter_value_list:
                        if filter_value is not None:
                            for item in filter_value:
                                if 'formulas' in item:
                                    formula_filter_results.extend(item['formulas'])

                                    # Extract names from formulas within the current item
                                    formula_names = [formula['name'] for formula in item['formulas'] if 'name' in formula]

                                    item_without_formulas = {k: v for k, v in item.items() if k != 'formulas'}
                                    non_formula_filter_results.append(item_without_formulas)

                                    for formula_name in formula_names:
                                        column_vals.append({
                                            'name': formula_name,
                                            'formula_id': formula_name,
                                            'properties': {
                                                'column_type': 'ATTRIBUTE',
                                                'index_type': 'DONT_INDEX'
                                            }
                                        })
                                else:
                                    non_formula_filter_results.append(item)
                else:
                    if 'filters' in mltml['model']:
                        del mltml['model']['filters']
                for i in range(len(model_df['Column Name'])):
                    for j in range(len(model_df['Column Name'][i])):
                        col_name = model_df['Column Name'][i][j].split('.')[-1]
                        col_name = col_name.split('(')[0].strip()
                        if col_name not in unique_cols:
                            unique_cols.append(model_df['Column Name'][i][j].split('.')[-1])
                            column_vals.append({
                                'name': col_name,
                                'column_id': string_concat(model_df['Table Name'][i], col_name),
                                'properties': {
                                    'column_type': model_df['Column Type'][i][j].upper(),
                                    'aggregation': model_df['Aggregation'][i][j].upper(),
                                    'index_type': 'DONT_INDEX'
                                }
                            })
                unique_cols.clear()
                mltml['model']['columns'] = column_vals

                if formula_filter_results:
                    formula_vals.extend(formula_filter_results)
                if non_formula_filter_results:
                    mltml['model']['filters'] = non_formula_filter_results
                else:
                    del mltml['model']['filters']
                mltml['model']['formulas'] = formula_vals

                # Handling joins
                joins_by_source = {}
                for i in range(model_df.shape[0]):
                    for j in range(len(model_df['Destination'][i])):
                        original_condition = model_df['Join Value'][i][j]
                        if not original_condition:
                            continue
                        join_parts = original_condition.split('::')

                        if len(join_parts) == 2:
                            table1_col = join_parts[0].replace('[', '').replace(']', '').split('.')
                            table2_col = join_parts[1].replace('[', '').replace(']', '').split('.')
                            join_condition = f"[{table1_col[0]}::{table1_col[1]}] = [{table2_col[0]}::{table2_col[1]}]"
                        else:
                            join_condition = original_condition

                        original_join_type = model_df['Join Type'][i][j].strip().upper()
                        ts_join_type = join_mapping_dict.get(original_join_type)

                        source_table = model_df['Table Name'][i]
                        destination_table = model_df['Destination'][i][j]
                        num_joins = model_df[model_df['Table Name'] == source_table].shape[0]
                        if num_joins == 1:
                            cardinality = 'ONE_TO_ONE'
                        elif num_joins == 2:
                            cardinality = 'ONE_TO_MANY'
                        else:
                            cardinality = 'MANY_TO_ONE'

                        wjoin = {
                            'with': destination_table,
                            'on': join_condition,
                            'type': ts_join_type,
                            'cardinality': cardinality
                        }

                        if source_table not in joins_by_source:
                            joins_by_source[source_table] = []
                        joins_by_source[source_table].append(wjoin)

                for table in mltml['model']['model_tables']:
                    table_name = table['name']
                    if table_name in joins_by_source:
                        table['joins'] = joins_by_source[table_name]
                    else:
                        table['joins'] = []

            combined_path = self.check_and_create_folder(output_path, "Model TML")
            model_instance = Model.loads(json.dumps(mltml))
            model_name = model_instance.name
            model_instance.dump(os.path.join(combined_path, f"{datasource}.model.tml"))

            get_logger().info(f"Model TML creation for '{datasource}' completed")
            return model_name+".model.tml"

        except Exception as e:
            get_logger().exception(f"Model TML creation failed for '{datasource}' : %s", e)


    def create_model_tml_for_extract(self, data_df, formula_details_df, datasource, output_path, tables):
        get_logger().info(f"Model TML creation started for the Extract datasource : '{datasource}'")

        try:
            datasource = re.sub(r'[^a-zA-Z0-9\s]', '', datasource)
            datasource = datasource.replace(' ', '_').lower()

            mltml = self.model_tml_template.to_dict()
            mltml['guid'] = ' '
            mltml['model']['name'] = datasource
            mltml['model']['model_tables'] = [{'name': datasource}]

            formula_vals = self.append_formulas(formula_details_df)

            def string_concat(name1, name2):
                return name1.upper() + '::' + name2

            column_vals = []
            added_tables = set()
            for table in tables:
                sliced_data_df = data_df[data_df['Table Name']==table]
                for _, row in sliced_data_df.iterrows():
                    if row['Remote Column Name'] in added_tables:
                        continue
                    added_tables.add(row['Remote Column Name'])
                    column_vals.append({
                                'name': row['Remote Column Name'],
                                'column_id': string_concat(datasource, row['Remote Column Name']),
                                'properties': {
                                    'column_type':row['Column Type'].upper(),
                                    'aggregation': row['Aggregation Type'].upper(),
                                    'index_type': 'DONT_INDEX'
                                }
                            })
            mltml['model']['columns'] = column_vals
            mltml['model']['formulas'] = formula_vals
            mltml['model']['filters'] = None
            combined_path = self.check_and_create_folder(output_path, "Model TML")
            model_instance = Model.loads(json.dumps(mltml))
            model_name = model_instance.name
            model_instance.dump(os.path.join(combined_path, f"{datasource}.model.tml"))

            get_logger().info(f"Model TML creation for '{datasource}' completed")
            return model_name+".model.tml", datasource

        except Exception as e:
            get_logger().exception(f"Model TML creation failed for '{datasource}' : %s", e)
        

    def check_and_create_folder(self, output_folder, folder_name):
        combined_path = os.path.join(output_folder, folder_name)
        if not os.path.exists(combined_path):
            os.makedirs(combined_path)
        return combined_path
    
    def append_formulas(self, formula_details_df):
        formula_vals = []
        for _, row in formula_details_df.iterrows():
                if row['formula_with_standardized_columns'] not in ["TBD", "error", "'NoneType' object has no attribute 'accept'"]:
                    if 'TBD' not in row['formula_with_standardized_columns']:
                        get_logger().info(f"Appending formula with the name '{row['local column name']}'")
                        formula_vals.append({
                            'id': row['local column name'],
                            'name': row['local column name'],
                            'expr': row['formula_with_standardized_columns']
                        })
                    else :
                        get_logger().error(f"Formula '{row['local column name']}' not supported as of now")
        return formula_vals