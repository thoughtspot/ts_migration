
class Datatype:

    def __init__(self):
        pass

    #Function to modify column name in standard format
    def modify_column(self,value1,value2):
        value2=value2.replace(value1,'').strip('()')
        value2=value1+'.'+value2
        return value2

    #Function to remove square bractes from a dataframe column
    def remove_sqbracket(self,value):
        value=value.strip('[]')
        return value

    #Function to find Datatype supported by TS or not
    def supported_info(self,value,datatype_map):
        ts_col_support=[]
        migrator_col_support=[]
        for i in value:
            if i in list(datatype_map['Tableau Data Type']):
                ival=datatype_map[datatype_map['Tableau Data Type']==i].index[0]
                ts=datatype_map['Supported by TS'][ival]
                migrator=datatype_map['Supported by Migrator'][ival]
            else:
                ts='No'
                migrator='No'
            ts_col_support.append(ts)
            migrator_col_support.append(migrator)
        return ts_col_support,migrator_col_support