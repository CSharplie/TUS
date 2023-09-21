
import re
import hashlib
import numpy as np
import pandas as pd
from datetime import datetime
from core.miscellaneous import print_log

def get_table_name(x):
    if(type(x["data"]) is dict and x["data"]["table_name"] is not None):
        return x["data"]["table_name"].upper()
    return None

def get_object_name(x):
    if(type(x["data"]) is dict and x["data"]["object_name"] is not None):
        return x["data"]["object_name"].upper()
    return None

def get_used_columns(row):
    regex = "[^&\.]\[([^]]*?)\]\.\[(.*?)\]|'([^']*?)'\[(.*?)\]|([\w_]+)\[(.*?)\]|[^&\.]\[(.*?)\]"
    result = []
    matches = re.findall(regex, row["query"])

    for match in matches:
        table_name = None
        object_name = None

        if match[0] != "":
            table_name = match[0]
            object_name = match[1]
        elif match[2] != "":
            table_name = match[2]
            object_name = match[3]   
        elif match[4] != "":
            table_name = match[4]
            object_name = match[5]   
        else:
            object_name = match[6]
        
        result.append({"table_name": table_name, "object_name": object_name})

    return result

def get_model_dependencies(df_objects):
    print_log("Calculate direct dependencies")

    df_objects["object_name"] = df_objects["object_name"]
    df_objects["table_name"] = df_objects["table_name"]

    df_dependencies = df_objects[df_objects["query"].notnull()].copy()
    df_dependencies["data"] = df_dependencies.apply(get_used_columns, axis = 1)
    df_dependencies = df_dependencies.explode("data")

    df_dependencies["referenced_table"] = df_dependencies.apply(get_table_name, axis = 1)
    df_dependencies["referenced_object"] = df_dependencies.apply(get_object_name, axis = 1)

    df_dependencies = df_dependencies.drop(["query", "table_id", "table_id", "object_id", "data"], axis = 1)
    df_objects = df_objects.drop(["query", "table_id", "object_id"], axis = 1)

    df_dependencies = pd.merge(df_dependencies, df_objects
                       , left_on = ["workspace_server", "dataset_database", "referenced_object"]
                       , right_on = ["workspace_server", "dataset_database", "object_name"]
                       , how = "left")

    df_dependencies["referenced_table"] = np.where(df_dependencies["referenced_table"].isnull(), df_dependencies["table_name_y"], df_dependencies["referenced_table"])

    df_dependencies = df_dependencies.rename(columns = { "object_type_x" : "object_type"
                                                        , "object_name_x" : "object_name"
                                                        , "table_name_x" : "table_name" 
                                                        , "object_type_y" : "referenced_object_type" })
    
    df_dependencies = df_dependencies.drop(["object_name_y", "table_name_y"], axis = 1)

    df_missing_dependencies = pd.merge(df_objects, df_dependencies
                       , left_on = ["workspace_server", "dataset_database", "object_name", "table_name"]
                       , right_on = ["workspace_server", "dataset_database", "object_name", "table_name"]
                       , how = "left")
    df_missing_dependencies = df_missing_dependencies[df_missing_dependencies["referenced_table"].isnull()]
    df_missing_dependencies = df_missing_dependencies.rename(columns = { "object_type_x" : "object_type" })
    df_missing_dependencies = df_missing_dependencies.drop(["object_type_y", "referenced_table", "referenced_object", "referenced_object_type"], axis = 1)

    df_dependencies = pd.concat([df_dependencies, df_missing_dependencies]).drop_duplicates().reset_index(drop = True)
 
    print_log("Analyse of sub dependencies")
    deep_analysis = True
    dept_level = 1
    while deep_analysis:
        print_log("Dept level {dept_level}".format(dept_level = dept_level))

        deep_analysis = False
        df_dependencies_to_add = None
        for index in df_dependencies.index:
            workspace_server = df_dependencies["workspace_server"][index]
            dataset_database = df_dependencies["dataset_database"][index]

            referenced_object_type = df_dependencies["referenced_object_type"][index]
            referenced_table = df_dependencies["referenced_table"][index]
            referenced_object = df_dependencies["referenced_object"][index]

            object_type = df_dependencies["object_type"][index]
            table_name = df_dependencies["table_name"][index]
            object_name = df_dependencies["object_name"][index]

            df_sub_dependencies = df_dependencies[(df_dependencies["workspace_server"] == workspace_server)
                            & (df_dependencies["dataset_database"] == dataset_database)
                            & (df_dependencies["object_type"] == referenced_object_type)
                            & (df_dependencies["table_name"] == referenced_table)
                            & (df_dependencies["object_name"] == referenced_object)
                            ].copy()
            
            if len(df_sub_dependencies) == 0:
                continue

            df_sub_dependencies = df_sub_dependencies.drop_duplicates()

            df_sub_dependencies["object_type"] = object_type
            df_sub_dependencies["table_name"] = table_name
            df_sub_dependencies["object_name"] = object_name

            count_with_add = len(pd.concat([df_sub_dependencies, df_dependencies]).drop_duplicates())
            count_without_add = len(df_dependencies)

            if count_with_add > count_without_add:
                if(df_dependencies_to_add is None):
                    df_dependencies_to_add = df_sub_dependencies
                else:
                    df_dependencies_to_add = pd.concat([df_dependencies_to_add, df_sub_dependencies]).drop_duplicates().reset_index(drop = True)

        if df_dependencies_to_add is not None:
            deep_analysis = True
            df_dependencies = pd.concat([df_dependencies, df_dependencies_to_add]).drop_duplicates().reset_index(drop = True)

        dept_level += 1

        df_dependencies = df_dependencies[df_dependencies["referenced_object_type"].notnull()]
    
    df_dependencies = df_dependencies.drop(["table_name_raw_x", "object_name_raw_x", "table_name_raw_y", "object_name_raw_y", "table_name_raw", "object_name_raw"], axis = 1)
    return df_dependencies


def get_parsed_queries(df_raw_queries, df_objects):
    df_raw_queries["data"] = df_raw_queries.apply(get_used_columns, axis = 1)
    df_working = df_raw_queries

    df_working["query_id"] = pd.RangeIndex(stop = df_working.shape[0])
    
    df_working = df_working.explode("data")

    df_working["table_name"] = df_working.apply(get_table_name, axis = 1)
    df_working["object_name"] = df_working.apply(get_object_name, axis = 1)

    df_working = df_working.drop(["query", "data"], axis = 1)

    df_query_level = df_working.drop_duplicates(["workspace_server", "dataset_database", "date_key", "table_name", "object_name", "query_id", "count"]).groupby(["workspace_server", "dataset_database", "date_key", "table_name", "object_name"], as_index = False).sum("count")
    df_query_level = df_query_level.rename(columns = {'count': 'count_query'})

    df_working = df_working.drop(["query_id"], axis = 1)
    
    df_column_level = df_working.groupby(["workspace_server", "dataset_database", "date_key", "table_name", "object_name"], as_index=  False)["count"].sum()
    df_column_level = df_column_level.rename(columns = {'count': 'count_call'})

    df_parsed_queries = pd.merge(df_query_level, df_column_level, on = ["workspace_server", "dataset_database", "date_key", "table_name", "object_name"], how = 'inner')
    
    df_parsed_queries = set_missing_tables(df_parsed_queries, df_objects)

    return df_parsed_queries


def set_missing_tables(df_parsed_queries, df_objects):
    df_with_object = pd.merge(df_parsed_queries[df_parsed_queries["table_name"].notnull()], df_objects, on = ["workspace_server", "dataset_database", "table_name", "object_name"], how = "inner")

    df_without_object = pd.merge(df_parsed_queries[df_parsed_queries["table_name"].isnull()], df_objects, on = ["workspace_server", "dataset_database", "object_name"], how = "inner")
    df_without_object["table_name"] = df_without_object["table_name_y"]
    df_without_object = df_without_object.drop(["table_name_x", "table_name_y"], axis = 1)

    return pd.concat([df_with_object, df_without_object]).drop(["query", "object_id", "table_id", "table_name_raw", "object_name_raw"], axis = 1)

