from thoughtspot_tml.utils import determine_tml_type
from thoughtspot_tml import Table
from thoughtspot_tml import SQLView
import json
import uuid
import os
from utilities.logger_config import get_logger

class TML_Generator:
     
    table_tml_template = None

    def __init__(self):
        self.table_tml_template = self.load_template("TEMPLATE.table.tml")
        self.sql_view_tml_template = self.load_template("TEMPLATE.sqlview.tml")

    def load_template(self, file_name):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_in_a_path = os.path.join(current_dir, 'tml_templates', file_name)
        table_tml_directory = determine_tml_type(path=file_in_a_path)
        return table_tml_directory.load(path=file_in_a_path)

    def generate_tml(self, data_df, custom_sql_df, table_map, output_path, connection_name):
        table_dict = {}
        view_dict = {}
        for table_name in data_df['Table Name'].unique():
            if custom_sql_df['Custom SQL Name'].isin([table_name]).any() :
                combined_path = self.check_and_create_folder(output_path, "SQL View TML")
                view_name = self.create_sql_view_tml(custom_sql_df, data_df, table_name, combined_path, connection_name)
                view_dict[table_name] = view_name
            else :
                combined_path = self.check_and_create_folder(output_path, "Table TML")
                table_full_name = table_map[table_name]
                parts = table_full_name.split('.')
                db = parts[0].strip('[]')
                schema = parts[1].strip('[]')
                tb_name = self.create_table_tml(table_name, data_df, db, schema, combined_path, connection_name)
                table_dict[table_name] = tb_name
        return table_dict, view_dict
    
    def check_and_create_folder(self, output_folder, folder_name):
        combined_path = os.path.join(output_folder, folder_name)
        if not os.path.exists(combined_path):
            os.makedirs(combined_path)
        return combined_path
     
    def create_table_tml(self, table_name, data_df, db, schema, output_path, connection_name):

        get_logger().info(f"Creating Table TML with the name of '{table_name}'")

        try :
            tbtml = self.table_tml_template.to_dict()
            tbtml['table']['name'] = table_name
            tbtml['table']['description'] = ""
            tbtml['table']['db_table'] = table_name
            tbtml['table']['db'] = db
            tbtml['table']['schema'] = schema
            tbtml['table']['connection']['name'] = connection_name
            # Assigning all the columns details and joins to the respected TML positions
            table_data = data_df[data_df['Table Name'] == table_name]
            columns = []
            column_set= set()
            for _, row in table_data.iterrows():
                if row['Remote Column Name'] in column_set:
                    continue
                column_set.add(row['Remote Column Name'])
                columns.append({
                    'name': row['Remote Column Name'],
                    'description': "",
                    'db_column_name': row['Remote Column Name'],
                    'properties': {'column_type': str.upper(row['Column Type']),
                                'aggregation': row['Aggregation Type'],
                                'index_type': 'DONT_INDEX'},
                    'db_column_properties': {'data_type': str.upper(row['Data Type'])}
                })
            tbtml['table']['columns'] = columns
            # Dump the current table's TML into a file with a unique name
            table_object = Table.loads(json.dumps(tbtml))
            output_file_name = output_path + "/" + table_name + ".table.tml"
            table_object.dump(output_file_name)
            tml_name = os.path.basename(output_file_name)
            get_logger().info(f"'{table_name}' creation completed")
            return tml_name
        except Exception as e:
            get_logger().exception(f"Table TML generation failed for '{table_name}' : %s", e)


    def create_sql_view_tml(self, custom_sql_df, data_df, table_name, output_path, connection_name) :

        get_logger().info(f"Creating SQL View TML with the name of '{table_name}'")
        
        try :
            sql_view_tml = self.sql_view_tml_template.to_dict()
            sql_view_tml['guid'] = str(uuid.uuid4())
            sql_view_tml['sql_view']['name'] = table_name

            query = custom_sql_df.loc[custom_sql_df['Custom SQL Name'] == table_name, 'Custom SQL Query'].values[0]
            sql_view_tml['sql_view']['sql_query'] = self.refactor_query(query)

            column_details = []
            column_set= set()
            table_data = data_df[data_df['Table Name'] == table_name]
            for _, row in table_data.iterrows():
                if row['Remote Column Name'] in column_set:
                    continue
                column_set.add(row['Remote Column Name'])
                columnProperty = {
                            "name": row['Remote Column Name'],
                            "sql_output_column" : row['Remote Column Name'],
                            "properties": {
                                "column_type" : str.upper(row['Column Type']),
                                "aggregation" : row['Aggregation Type'],
                                "index_type" : 'DONT_INDEX'
                                }
                            }
                column_details.append(columnProperty)
            
            sql_view_tml['sql_view']['sql_view_columns'] = column_details
            sql_view_tml['sql_view']['connection']['name'] = connection_name
            
            SQLView.loads(json.dumps(sql_view_tml)).dump(output_path + "/" + table_name + ".sqlview.tml")
            get_logger().info("SQL View TML creation completed")
            return table_name+".sqlview.tml"
        except Exception as e:
            get_logger().exception(f"SQL view TML generation failed for '{table_name}' : %s", e)

    def refactor_query(self, query):
        return query.replace('<<', '<').replace('>>', '>').replace('==', '=')
     