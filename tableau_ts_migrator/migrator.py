import pandas as pd
import numpy as np
from tableau_ts_migrator.tml_generator import TML_Generator
from tableau_ts_migrator.sql_query_generator import Sql_Query_Generator
from collections import deque
from tableau_ts_migrator.filter_migrator import Filter_Migrator
from utilities import file_reader
import os
from tableau_ts_migrator.generate_model_tml import ModelGenerate
from tableau_ts_migrator.generate_live_filters import LiveDataFilter
import re
from utilities.logger_config import get_logger
from itertools import cycle


class Migrator:

    def __init__(self):
        self.datatype_map = file_reader.get_mapping_file()["datatype.csv"]



    def migrate(self, parsed_dump, output_folder):
        # Collecting all TWB file names
        twb_file_names = parsed_dump["twb file name"].unique().tolist()
        dump = self.convert_datasources(
            parsed_dump, twb_file_names, self.datatype_map, output_folder
        )
        return dump

    def convert_datasources(
        self, parsed_dump, twb_file_names, datatype_map, output_folder
    ):

        for twb_file in twb_file_names:
            # Slice dataframe based on file name
            get_logger().info(f"Conversion started for the TWB file : '{twb_file}'")
            sliced_df_with_twb_name = parsed_dump[
                parsed_dump["twb file name"] == twb_file
            ]

            # Collect all the datasources present in the twb
            datasource_names = (
                sliced_df_with_twb_name["datasource name"].unique().tolist()
            )
            datasource_names = [x for x in datasource_names if pd.notna(x)]

            for datasource in datasource_names:

                get_logger().info(
                    f"Conversion in progress for the datasource '{datasource}' from the TWB file '{twb_file}'"
                )

                try:
                    # Slice dataframe based on datasource name
                    sliced_df_with_ds_name = sliced_df_with_twb_name[
                        sliced_df_with_twb_name["datasource name"] == datasource
                    ]
                    datasource_type = sliced_df_with_ds_name[
                        sliced_df_with_ds_name["property type"] == "ds_type"
                    ]["property value"].tolist()[0]

                    is_published_datasource = datasource_type == "Published Datasource"

                    #Added for Converting published datasources to Live 
                    
                    if is_published_datasource:
                        get_logger().info(
                            f"Datasource '{datasource}' is a published datasource; treating as Live for TML generation"
                        )
                        datasource_type = "Live"

                    for cdw_type in sliced_df_with_ds_name[
                        sliced_df_with_ds_name["property type"] == "named_connection"
                    ]["property value"].tolist():
                        if cdw_type != "snowflake":
                            raise ValueError(
                                f"Migration not supported for {cdw_type}. Only supported for snowflake. Unable to process the datasource '{datasource}' which belongs to the file '{twb_file}'."
                            )

                    # Retrieve tables and custom sql names
                    table_names = sliced_df_with_ds_name[
                        (sliced_df_with_ds_name["object type"] == "table")
                        | (sliced_df_with_ds_name["object type"] == "custom sql query")
                    ]["table name"].tolist()

                    #added fallback code  for sqlproxies to get table name  from columns 
                    if not table_names:
                        column_table_names = (
                            sliced_df_with_ds_name[
                                (sliced_df_with_ds_name["object type"] == "column")
                                & (
                                    sliced_df_with_ds_name["property type"]
                                    == "metadata-record"
                                )
                            ]["table name"]
                            .dropna()
                            .unique()
                            .tolist()
                        )
                        table_names = column_table_names

                    # Retrieve custom sql names alone
                    custom_sql_names = sliced_df_with_ds_name[
                        (sliced_df_with_ds_name["object type"] == "custom sql query")
                    ]["table name"].tolist()
                    table_map = {}
                    table = []
                    table_full_name = []
                    remote_columns = []  # Database columns
                    local_columns = []  # Tableau local columns
                    data_type = []
                    column_type = []
                    aggregation = []
                    custom_sql_queries = []
                    table_column_relationship_map = {}
                    #added code to hanlde the table names for sqlproxies 
                    for t in table_names:
                        parts = t.split(".")
                        formatted_full_name = t
                        raw_table_token = parts[2] if len(parts) >= 3 else parts[0]
                        table_name = raw_table_token.strip("[]")

                        if len(parts) < 3:
                            formatted_full_name = raw_table_token.strip("[]")

                        # Table name and Table full name into dictionary
                        table_map[table_name] = formatted_full_name

                        #  queries to form custom sql dataframe if the table is a custom sql
                        if t in custom_sql_names:
                            query = sliced_df_with_ds_name[
                                (
                                    sliced_df_with_ds_name["object type"]
                                    == "custom sql query"
                                )
                                & (sliced_df_with_ds_name["table name"] == t)
                            ]["property value"]
                            custom_sql_queries.append(query.iloc[0])

                        column_details_df = sliced_df_with_ds_name[
                            (sliced_df_with_ds_name["object type"] == "column")
                            & (sliced_df_with_ds_name["table name"] == raw_table_token)
                            & (
                                sliced_df_with_ds_name["property type"]
                                == "metadata-record"
                            )
                        ]
                        for _, row in column_details_df.iterrows():
                            table.append(table_name)
                            table_full_name.append(table_map[table_name])
                            remote_columns.append(row["remote column name"])
                            local_columns.append(row["local column name"])

                            index_number = datatype_map[
                                datatype_map["Tableau Data Type"]
                                == row["property value"]
                            ].index[0]
                            data_type.append(datatype_map["TS Data Type"][index_number])
                            column_type.append(
                                datatype_map["TS Column Type"][index_number]
                            )
                            aggregation.append(
                                datatype_map["TS Aggregation Type"][index_number]
                            )

                            table_column_relationship_map[
                                row["remote column name"].strip("[]")
                            ] = table_name

                    formula_details_df = sliced_df_with_ds_name[
                        (sliced_df_with_ds_name["object type"] == "column")
                        & (
                            sliced_df_with_ds_name["property type"]
                            == "calculated_column"
                        )
                    ]
                    # Creating a dataframe to store all the metadata details
                    data_df = pd.DataFrame(
                        np.column_stack(
                            [
                                table,
                                table_full_name,
                                remote_columns,
                                local_columns,
                                data_type,
                                column_type,
                                aggregation,
                                [x + "::" + y for x, y in zip(table, remote_columns)], 
                            ]
                        ),
                        columns=[
                            "Table Name",
                            "Table Full Name",
                            "Remote Column Name",
                            "Local Column Name",
                            "Data Type",
                            "Column Type",
                            "Aggregation Type",
                            "column_id",
                        ],
                    )
                    formula_details_df["formula_with_standardized_columns"] = None
                    for index, row in formula_details_df.iterrows():
                        formula_details_df.at[
                            index, "formula_with_standardized_columns"
                        ] = replace_bracketed_values(
                            row["conversion in TS"], data_df, datasource_type
                        )
                    # Creating a dataframe to store the custom sql details
                    custom_sql_df = pd.DataFrame(
                        np.column_stack([custom_sql_names, custom_sql_queries]),
                        columns=["Custom SQL Name", "Custom SQL Query"],
                    )

                    # Slicing the join details
                    sliced_joins_df = sliced_df_with_ds_name[
                        (sliced_df_with_ds_name["object type"] == "datasource property")
                        & (sliced_df_with_ds_name["property type"] == "join")
                    ][["property name", "property value"]]
                    source = []
                    source_full = []
                    destination = []
                    destination_full = []
                    join_type = []
                    join_name = []
                    join_val = []
                    join_map = {}

                    # If there are no joins present, append source information only
                    if sliced_joins_df.empty:
                        get_logger().info(
                            f"No joins present in the datasource '{datasource}'"
                        )
                        for key, value in table_map.items():
                            source.append(key)
                            source_full.append(value)
                            destination.append(None)
                            destination_full.append(None)
                            join_type.append(None)
                            join_name.append(None)
                            join_val.append(None)
                    else:
                        # Iterate the join information present and retrieve the necessary details from it.
                        get_logger().info("Processing Joins and creating a dataframe")
                        for _, row in sliced_joins_df.iterrows():

                            # Join column
                            fk_join = row["property value"]
                            fk_join_arr = fk_join.split("::")

                            source_table_col = re.findall(r"\[[^\]]+\]", fk_join_arr[0])
                            dest_table_col = re.findall(r"\[[^\]]+\]", fk_join_arr[1])

                            source_table = source_table_col[0].strip("[]")
                            dest_table = dest_table_col[0].strip("[]")

                            source.append(source_table)
                            source_full.append(table_map[source_table])
                            destination.append(dest_table)
                            destination_full.append(table_map[dest_table])
                            join_type.append(row["property name"])
                            join_name.append(source_table + "_to_" + dest_table)
                            join_val.append(fk_join)

                            if source_table not in join_map:
                                join_map[source_table] = []

                            if dest_table not in join_map:
                                join_map[dest_table] = []

                            join_map[source_table].append(dest_table)
                            join_map[dest_table].append(source_table)

                    join_df = pd.DataFrame(
                        np.column_stack(
                            [
                                source,
                                source_full,
                                destination,
                                destination_full,
                                join_type,
                                join_name,
                                join_val,
                            ]
                        ),
                        columns=[
                            "Source",
                            "Source Full",
                            "Destination",
                            "Destination Full",
                            "Join Type",
                            "Join Name",
                            "Join Value",
                        ],
                    )

                    # Getting all the tables present
                    all_tables = self.return_table_list(join_df)

                    split_tables = self.split_tables(all_tables, join_map)

                    # Retrieve the filter details as a dataframe from the sliced datasource df
                    filters_df = sliced_df_with_ds_name[
                        (sliced_df_with_ds_name["object type"] == "datasource property")
                        & sliced_df_with_ds_name["property type"]
                        .fillna("")
                        .str.startswith("Filter")
                    ]

                    # If datasource is Live create TML, else create SQL query
                    if datasource_type == "Live":
                        get_logger().info(f"'{datasource}' is Live")
                        output_path = self.check_and_create_folder(
                            output_folder, "Live"
                        )
                        output_file = os.path.join(output_folder, "combined_output.csv")

                        tml_gen = TML_Generator()
                        table_dict, view_dict = tml_gen.generate_tml(
                            data_df,
                            custom_sql_df,
                            table_map,
                            output_path,
                            # "dgprojectTS",
                            "",
                            file_name_prefix="sqlproxy_" if is_published_datasource else "",
                        )
                        #utility inner function to add table rows for sqlproxy/published datasources that only had column metadata rows
                        def _append_missing_table_rows(table_dict_input):

                            if sliced_df_with_ds_name.empty:
                                return
                            sample_row = sliced_df_with_ds_name.iloc[0]
                            for table_key, output_name in table_dict_input.items():
                                existing = parsed_dump[
                                    (parsed_dump["object type"] == "table")
                                    & (parsed_dump["datasource name"] == datasource)
                                    & (
                                        parsed_dump["table name"]
                                        .fillna("")
                                        .apply(lambda x: x.split(".")[-1].strip("[]"))
                                        == table_key
                                    )
                                ]
                                if not existing.empty:
                                    continue
                                # Build a new row using the existing schema
                                new_row = {col: sample_row.get(col, None) for col in parsed_dump.columns}
                                new_row["object type"] = "table"
                                new_row["datasource name"] = datasource
                                new_row["table name"] = table_key
                                new_row["property type"] = ""
                                new_row["property name"] = ""
                                new_row["property value"] = ""
                                new_row["conversion in TS"] = "Thoughtspot table"
                                new_row["supported in TS"] = "Full"
                                new_row["supported in Migrator"] = "Full"
                                new_row["output_file_type"] = "Table TML"
                                new_row["output_file_name"] = output_name
                                parsed_dump.loc[len(parsed_dump)] = new_row

                        _append_missing_table_rows(table_dict)

                        output_file = os.path.join(output_folder, "combined_output.csv")
                        def _set_table_output(table_key, output_name):
                            # Prefer rows with matching datasource + table name; fall back to any metadata row for that table.
                            base_mask = parsed_dump["datasource name"] == datasource
                            name_mask = (
                                parsed_dump["table name"]
                                .fillna("")
                                .apply(lambda x: x.split(".")[-1].strip("[]"))
                                == table_key
                            )
                            idx = parsed_dump.index[base_mask & name_mask]
                            if len(idx) == 0:
                                # fallback: use first metadata row for this datasource
                                idx = parsed_dump.index[
                                    base_mask
                                    & (parsed_dump["property type"].fillna("") == "metadata-record")
                                ]
                            if len(idx) > 0:
                                parsed_dump.at[idx[0], "output_file_type"] = "Table TML"
                                parsed_dump.at[idx[0], "output_file_name"] = output_name

                        for extracted_table_name, tml_name in table_dict.items():
                            _set_table_output(extracted_table_name, tml_name)

                        view_mask = parsed_dump["object type"] == "custom sql query"
                        parsed_dump.loc[view_mask, "output_file_type"] = "SQL View TML"

                        for index, row in parsed_dump[view_mask].iterrows():
                            table_name = row["table name"]
                            extracted_view_name = table_name.split(".")[-1].strip("[]")

                            if extracted_view_name in view_dict:
                                parsed_dump.at[index, "output_file_name"] = view_dict[
                                    extracted_view_name
                                ]

                        # parsed_dump.to_csv(output_file, mode='a', header=False, index=False)

                        if is_published_datasource:
                            get_logger().info(
                                f"Published datasource '{datasource}'; generating model/worksheet TML as Live"
                            )

                        filters = LiveDataFilter()
                        live_filters = filters.live_filter(
                            filters_df, table_column_relationship_map, datasource
                        )
                        if is_published_datasource:
                            get_logger().info(
                                f"Skipping model/worksheet TML generation for published datasource '{datasource}' (guarded by SKIP_PUBLISHED_MODEL_TMLS)"
                            )
                        else:
                            model_dump = self.create_model_tml(
                                data_df,
                                join_df,
                                formula_details_df,
                                datasource,
                                output_path,
                                split_tables,
                                True,
                                live_filters,
                            )
                            output_file = os.path.join(output_folder, "combined_output.csv")
                            model_mask = (parsed_dump["property type"] == "ds_type") & (
                                parsed_dump["datasource name"] == datasource
                            )
                            parsed_dump.loc[model_mask, "output_file_type"] = (
                                "Worksheet TML"
                            )
                            for index, row in parsed_dump[model_mask].iterrows():
                                datasource_name = row["datasource name"]
                                matching_key = None
                                for key in model_dump.keys():
                                    if key.startswith(datasource_name):
                                        matching_key = key
                                        break

                                if matching_key:
                                    parsed_dump.at[index, "output_file_name"] = model_dump[
                                        matching_key
                                    ]
                                    print(
                                        f"Updated row {index} with output_file_name: {model_dump[matching_key]}"
                                    )
                                else:
                                    print(
                                        f"Warning: '{datasource_name}' not found in model_dump"
                                    )
                            # parsed_dump.to_csv(output_file, mode='a', header=False, index=False)

                    elif datasource_type == "Extract":
                        get_logger().info(f"'{datasource}' is Extract")
                        output_path = self.check_and_create_folder(
                            output_folder, "Extract"
                        )
                        sql_gen = Sql_Query_Generator()
                        filter_migrator = Filter_Migrator()
                        for tables in split_tables:
                            # Filtering out necessary details from join_df
                            table_relationships = self.filter_relationships(
                                join_df, tables
                            )

                            # Form select column and cte queries
                            final_cte_query, select_column_query = (
                                sql_gen.generate_sql_query(
                                    tables,
                                    data_df,
                                    join_df,
                                    custom_sql_df,
                                    table_relationships,
                                    table_map,
                                )
                            )

                            # Form filter queries
                            join_query, where_query, cte_query = (
                                filter_migrator.form_filter_queries(
                                    filters_df,
                                    table_column_relationship_map,
                                    datasource,
                                )
                            )

                            # Append both queries
                            generated_sql_query = self.form_full_query(
                                select_column_query,
                                cte_query,
                                final_cte_query,
                                join_query,
                                where_query,
                            )

                            # Create the file and write the SQL query to it
                            sql_file = (
                                re.sub(r"[^a-zA-Z0-9\s]", "", datasource)
                                .replace(" ", "_")
                                .lower()
                            )
                            sql_file_name = sql_file + ".sql"
                            # locate indexes with True or False values to update the output_file_type and output_file_name
                            sql_mask = (parsed_dump["object type"] == "table") & (
                                parsed_dump["conversion in TS"] == "Mode Dataset"
                            )
                            sql_mask_file_name = (
                                (parsed_dump["object type"] == "table")
                                & (parsed_dump["conversion in TS"] == "Mode Dataset")
                                & (parsed_dump["datasource name"] == datasource)
                            )
                            # create a directory to store the SQL files in the output folders
                            combined_path = self.check_and_create_folder(
                                output_path, "SQL Files"
                            )
                            sql_file_path = os.path.join(combined_path, sql_file_name)
                            with open(sql_file_path, "w") as file:
                                file.write(generated_sql_query)

                            parsed_dump.loc[sql_mask, "output_file_type"] = "SQL files"
                            parsed_dump.loc[sql_mask_file_name, "output_file_name"] = (
                                sql_file_name
                            )

                            get_logger().info(
                                f"Extract SQL query file generated with the name '{sql_file_name}'"
                            )

                        model_dump = self.create_model_tml(
                            data_df,
                            join_df,
                            formula_details_df,
                            datasource,
                            output_path,
                            split_tables,
                            False,
                            None,
                        )
                        model_mask = (parsed_dump["property type"] == "ds_type") & (
                            parsed_dump["property value"] == "Extract"
                        )
                        parsed_dump.loc[model_mask, "output_file_type"] = "SQL files"

                        def normalize_string(s):
                            return re.sub(r"[^a-zA-Z0-9]", "", s).lower()

                        for index, row in parsed_dump[model_mask].iterrows():
                            datasource_name = row["datasource name"]
                            normalized_datasource_name = normalize_string(
                                datasource_name
                            )

                            matching_key = None
                            for key in model_dump.keys():
                                normalized_key = normalize_string(key)
                                if normalized_key.startswith(
                                    normalized_datasource_name
                                ):
                                    matching_key = key
                                    break

                            if matching_key:
                                parsed_dump.at[index, "output_file_name"] = model_dump[
                                    matching_key
                                ]
                                print(
                                    f"Updated row {index} with output_file_name: {model_dump[matching_key]}"
                                )
                            else:
                                print(
                                    f"Warning: '{datasource_name}' not found in model_dump"
                                )
                        # parsed_dump.to_csv(output_file, mode='a', header=False, index=False)
                except Exception as e:
                    get_logger().exception(e)
        return parsed_dump

    def create_model_tml(
        self,
        data_df,
        join_df,
        formula_details_df,
        datasource,
        output_path,
        split_tables,
        create_joins,
        live_filters,
    ):
        object_model = ModelGenerate()
        model_dump = {}

        if create_joins:
            for table in split_tables:
                model_df = object_model.create_join_df(
                    table, data_df, join_df, datasource
                )
                model_tml_file = object_model.create_model_tml(
                    model_df, formula_details_df, datasource, output_path, live_filters
                )
                key = datasource
                model_dump[key] = model_tml_file
        else:
            for table in split_tables:
                model_tml_file, datasource = object_model.create_model_tml_for_extract(
                    data_df, formula_details_df, datasource, output_path, table
                )
                key = datasource
                model_dump[key] = model_tml_file
        return model_dump

    def check_and_create_folder(self, output_folder, folder_name):
        combined_path = os.path.join(output_folder, folder_name)
        if not os.path.exists(combined_path):
            os.makedirs(combined_path)
        return combined_path

    # Form the full query
    def form_full_query(
        self, select_column_query, cte_query, final_cte_query, join_query, where_query
    ):
        if cte_query:
            final_cte_query = final_cte_query + ",\n" + cte_query
        if join_query:
            select_column_query = select_column_query + join_query
        if where_query:
            select_column_query = select_column_query + where_query
        return final_cte_query + "\n" + select_column_query + ";"

    def return_table_list(self, join_df):
        sources = [
            source for source in join_df["Source"].unique() if source is not None
        ]
        dests = [dest for dest in join_df["Destination"].unique() if dest is not None]
        tables = set(sources + dests)
        return tables

    def split_tables(self, all_tables, join_map):
        grouped_tables = []
        visited = set()

        # Start BFS from each node to ensure all nodes are processed
        for node in all_tables:
            if node not in visited:
                table_group = self.findGroupBFS(node, join_map, visited)
                grouped_tables.append(table_group)

        return grouped_tables

    # Breadth First Search approach to find all the connected tables
    def findGroupBFS(self, table, join_map, visited):
        q = deque([table])
        table_group = []

        while q:
            # Poll current table from Queue.
            current_table = q.popleft()

            # Check if the table already added.
            if current_table not in visited:
                visited.add(current_table)
                table_group.append(current_table)

                # Add the tables connected with current table.
                for connected_table in join_map.get(current_table, []):
                    if connected_table not in visited:
                        q.append(connected_table)

        return table_group

    # Filtering out the necessary details from join dataframe
    def filter_relationships(self, join_df, tables):
        relationships = []
        for _, row in join_df.iterrows():
            if row["Source"] in tables or row["Destination"] in tables:
                relationship = {
                    "Source": row["Source"],
                    "Destination": row["Destination"],
                    "Join Type": row["Join Type"],
                    "Join Value": row["Join Value"],
                }
                relationships.append(relationship)
        return relationships


def replace_bracketed_values(input_string, column_details_df, datasource_type="Live"):

    # Regular expression to match content in square brackets and parentheses
    pattern = r"\[(.*?)\]"

    # Replace the content inside square brackets
    output_string = re.sub(
        pattern,
        lambda x: replace_formula_with_standardized_columns(
            x.group(1), column_details_df, datasource_type
        ),
        input_string,
    )
    return output_string


def replace_formula_with_standardized_columns(formula, data_df, datasource_type):
    result = data_df[data_df["Local Column Name"] == formula]
    if result.empty:
        return "[" + formula + "]"
    if datasource_type == "Live":
        return "[" + str(result["column_id"].iloc[0]).rsplit(" (", 1)[0] + "]"
    return f"[{str(result['Remote Column Name'].iloc[0])}]"
