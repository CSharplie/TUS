from datetime import date, datetime, timedelta
import hashlib
import os
import pandas as pd

def set_object_hash_key(df):
    df["concat"] = df["workspace_server"].astype(str) + df["dataset_database"].astype(str) + df["table_name_raw"].astype(str) + df["object_name_raw"].astype(str) + df["object_type"].astype(str)
    df["object_key"] = df["concat"].apply(lambda x: hashlib.md5(x.encode()).hexdigest())

    df = df.drop(["concat"], axis = 1)

    return df

def get_output_path(export_name):
    return "output/{name}.csv".format(name = export_name)

def export_output(df_export, export_name, partionned):
    path = get_output_path(export_name)
    if partionned:
        export_partitions = df_export["date_key"].drop_duplicates().to_numpy()
        if os.path.isfile(path):
            df_source = pd.read_csv(path)
            df_source = df_source[~df_source["date_key"].isin(export_partitions)]

            df_export = pd.concat([df_export, df_source])

        df_export = df_export.sort_values(by = "date_key")
    df_export.to_csv(path, index = False)

def get_output_objects(df_objects):
    df_output_object = set_object_hash_key(df_objects)

    df_output_object = df_output_object[["object_key", "workspace_server", "dataset_database", "object_type", "table_name_raw", "object_name_raw"]]
    df_output_object = df_output_object.rename(columns = {"table_name_raw" : "table_name", "object_name_raw" : "object_name"})

    return df_output_object

def get_output_usage_by_object(df_parsed_queries, df_objects, df_dependencies):
    first_date = datetime.strptime(df_parsed_queries["date_key"].min(), "%Y-%m-%d")
    last_date = datetime.strptime(df_parsed_queries["date_key"].max(), "%Y-%m-%d")
    df_dates = pd.DataFrame({ "date_key": pd.date_range(start = first_date, end = last_date) })
    df_dates["date_key"] = df_dates["date_key"].astype(str)

    df_objects = df_objects.drop(["query", "table_id", "object_id"], axis = 1)  

    df_output_object = pd.merge(df_dates, df_objects, how = "cross")
    df_output_object = pd.merge(df_output_object, df_parsed_queries
                                , on = ["workspace_server", "dataset_database", "table_name", "object_name", "date_key"]
                                , how = "left")
    
    df_output_object = df_output_object.drop(["query_id"], axis = 1)
    df_output_object = df_output_object.rename(columns = { "count_call": "direct_number_of_execution", "count_query": "direct_number_of_queries" })

    df_dependencies_usage = df_dependencies.merge(df_output_object
                                                  , on = ["workspace_server", "dataset_database", "table_name", "object_name"]
                                                  , how = "inner"
                                                  )
    df_dependencies_usage = df_dependencies_usage.drop(["object_type_x", "object_type_y", "table_name_raw", "object_name_raw"], axis = 1)
    df_dependencies_usage = df_dependencies_usage.groupby(["workspace_server", "dataset_database", "date_key", "referenced_table", "referenced_object"], as_index = False).agg({"direct_number_of_execution" : "sum"})
    df_dependencies_usage = df_dependencies_usage.rename(columns = { "direct_number_of_execution" : "indirect_number_of_execution", "referenced_table" : "table_name", "referenced_object" : "object_name"})

    df_output_object = df_output_object.merge(df_dependencies_usage
                                              , on = ["workspace_server", "dataset_database", "table_name", "object_name", "date_key"]
                                              , how = "left"
                                              )
    df_output_object = df_output_object.rename(columns = { "object_type_x" : "object_type"})

    df_output_object = set_object_hash_key(df_output_object)

    df_output_object = df_output_object.fillna(0)
    df_output_object = df_output_object[["date_key", "object_key", "direct_number_of_queries", "direct_number_of_execution", "indirect_number_of_execution"]]
    df_output_object = df_output_object.astype({ "direct_number_of_queries" : "int", "direct_number_of_execution" : "int", "indirect_number_of_execution" : "int" })

    return df_output_object


def get_output_storage(df_storage, df_parsed_queries):
    path = get_output_path("storage")
    if not os.path.isfile(path):
        first_date = datetime.strptime(df_parsed_queries["date_key"].min(), "%Y-%m-%d")
        last_date = datetime.strptime(df_parsed_queries["date_key"].max(), "%Y-%m-%d")
    else:
        df_source = pd.read_csv(path)
        first_date = (datetime.strptime(df_source["date_key"].max(), "%Y-%m-%d") + timedelta(days = 1)).date()
        last_date = datetime.now().date()

    if last_date < first_date:
        first_date = last_date

    df_dates = pd.DataFrame({ "date_key": pd.date_range(start = first_date, end = last_date) })
    df_dates["date_key"] = df_dates["date_key"].astype(str)

    df_output_storage = pd.merge(df_dates, df_storage, how = "cross")
    df_output_storage = set_object_hash_key(df_output_storage)

    df_output_storage = df_output_storage.fillna(0)
    df_output_storage = df_output_storage[["date_key", "object_key", "dictionary_size", "used_size"]]
    df_output_storage = df_output_storage.astype({ "used_size" : "int", "dictionary_size" : "int" })

    return df_output_storage