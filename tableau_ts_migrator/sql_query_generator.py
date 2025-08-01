from collections import deque
from utilities.logger_config import get_logger

class Sql_Query_Generator:

    def __init__(self) -> None:
        pass
      
    def generate_sql_query(self, tables, data_df, join_df, custom_sql_df, table_relationships, table_full_name_map):

        # Adding the columns of all tables
        try:
            columns = list(self.column_info(data_df, tables))

            # Separating the Foreign Key columns
            join_columns = self.join_info(join_df, tables)

            # Removing duplicate Foreign Key columns from total columns and appending them.
            final_columns = self.final(columns, join_columns)
            combined_column_query = ', \n'.join(final_columns)

            cte_queries, select_columns_query = self.generate_table_join_query(combined_column_query, tables, custom_sql_df, table_relationships, table_full_name_map)
            final_cte_query = "with "+',\n'.join(cte_queries)
            return final_cte_query, select_columns_query

        except Exception as e:
            get_logger().exception(f"SQL query creation failed : %s", e)

    def generate_table_join_query(self, combined_column_query, all_tables, custom_sql_df, table_relationships, table_full_name_map):

        get_logger().info("Generating query with columns and joins")

        cte_queries = []

        # Add all of the custom sql queries present in the datasource.
        for _, row in custom_sql_df.iterrows():
            cte_queries.append(r"{} AS ({})".format(row['Custom SQL Name']
                                                                  .replace(" ", ""), row['Custom SQL Query']
                                                                  .replace(">>", ">")
                                                                  .replace("<<", "<").replace("==", "="))) 
        
        # Add other tables to the common table expression queries array only if the table is not custom sql.
        for t in all_tables:
            if t not in custom_sql_df['Custom SQL Name'].tolist():
                cte_queries.append(("{} as (SELECT * FROM {})".format(t.replace(" ", ""), self.name_modify(table_full_name_map[t]))))

        # convert the relationship as a Queue
        queue = deque(table_relationships)

        # Pop the first relationship, which would act as a main table query
        relationship = queue.popleft()
        s = relationship['Source']
        d = relationship['Destination']
        referenced = set()
        referenced.add(s)
        referenced.add(d)

        source = table_full_name_map[s]
        new_s_name = self.name_modify(source)

        # Append the relationship details to the first line query
        select_columns_query = r"SELECT {} FROM {}".format(combined_column_query,new_s_name.replace(" ", ""))

        join_type = None
        join_des = None
        join_val = None
        if d is not None: 
            join_type = relationship['Join Type'].upper()
            join_des = relationship['Destination']
            join_val = relationship['Join Value'].replace('[','').replace(']','').replace(' ','').replace('::',' = ')
            select_columns_query = select_columns_query + "\n"+  "{} JOIN {} ON {}".format(join_type,join_des,join_val)

        # Iterate through the queue and add join query for all the remaining relationships
        # If a relationship is not referenced before in the query, it will be again added to the queue. 
        # The query will be created only after the source or destination is referenced previously.
        while queue:
            rel = queue.popleft()
            key = rel['Source']
            value = rel['Destination']

            if (key is None or value is None) or (key in referenced and value in referenced):
                continue
            if key in referenced:
                join_type = rel['Join Type'].upper()
                join_val = rel['Join Value'].replace('[','').replace(']','').replace(' ','').replace('::',' = ')
                select_columns_query += "\n" + "{} JOIN {} ON {}".format(join_type,value,join_val)
                referenced.add(value)
            elif value in referenced:
                join_type = rel['Join Type'].upper()
                join_val = rel['Join Value'].replace('[','').replace(']','').replace(' ','').replace('::',' = ')
                select_columns_query += "\n" + "{} JOIN {} ON {}".format(join_type,key,join_val)
                referenced.add(key)
            else :
                queue.append(rel)

        get_logger().info("Columns and Joins query creation completed")
        return cte_queries, select_columns_query
      

    def column_info(self, data_df,table):   
        columns=[]
        for i in range(data_df.shape[0]):
            if data_df['Table Name'][i] in table:
                col=data_df['column_id'][i].replace('::','.')
                columns.append(col.replace(" ", ""))
        return set(columns)

    def join_info(self, join_df, tables):
        join_columns=[]
        for i in range(join_df.shape[0]):
            if join_df['Source'][i] in tables and join_df['Join Value'][i] is not None:
                j_col=(join_df['Join Value'][i].split('::')[-1]).replace('[','').replace(']','')
                join_columns.append(j_col)
        return join_columns

    def final(self, columns,join_columns):
        final_columns=[]
        for i in columns:
            if i not in join_columns:
                final_columns.append(i)
        return final_columns

    def name_modify(self, source):
        tb_name_list=source.split('.')
        l=[]
        for i in tb_name_list:
            l.append(i.strip('[]'))
        name='.'.join(l)
        return name