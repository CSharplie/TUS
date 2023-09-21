import os
import re
import pandas as pd
from pyadomd import Pyadomd
from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
from azure.core.exceptions import HttpResponseError
from core.miscellaneous import print_log

def set_auth_environment_variables(configuration):
    os.environ["AZURE_TENANT_ID"] = configuration["azure"]["tenant_id"]
    os.environ["AZURE_CLIENT_ID"] = configuration["azure"]["application_id"]
    os.environ["AZURE_CLIENT_SECRET"] = configuration["azure"]["secret_key"]

# Azure logs analytics 
def execute_azure_log_analytics_query(query, worspace_id): 
    start_time = datetime(1900, 1, 1, tzinfo = timezone.utc)
    end_time = datetime(9999, 12, 31, tzinfo = timezone.utc)

    credential = DefaultAzureCredential()
    client = LogsQueryClient(credential)

    try:
        response = client.query_workspace(
            workspace_id = worspace_id,
            query = query,
            timespan = (start_time, end_time)
            )
        if response.status == LogsQueryStatus.PARTIAL:
            data = response.partial_data
            print_log("Partial query", 30)
            print_log(response.partial_error, 30)

        elif response.status == LogsQueryStatus.SUCCESS:
            data = response.tables

        for table in data:
            df = pd.DataFrame(data = table.rows, columns = table.columns)
            return df

    except HttpResponseError as err:
        print_log(err, 50)
        exit(1)


def get_log_analytics_raw_queries(queries, configuration):
    current_mode = configuration["mode"]

    partitions = execute_azure_log_analytics_query(queries["azure_log_analytics"][current_mode]["get_available_partitions"], configuration["log_analytics"]["workspace_id"])
    all_log_analytics_queries = None
    for partition in partitions.partition:
        partition_date = datetime.strptime(partition[:10], "%Y-%m-%d")
        current_date = datetime.today()

        if (current_date - partition_date).days > configuration["log_analytics"]["search_dept"]:
            continue

        print_log("Get queries for partiton '{partition}'".format(partition = partition))
        query = queries["azure_log_analytics"][current_mode]["get_queries"].format(partition = partition)
        log_analytics_queries = execute_azure_log_analytics_query(query, configuration["log_analytics"]["workspace_id"])

        if all_log_analytics_queries is None:
            all_log_analytics_queries = log_analytics_queries
        else:
            all_log_analytics_queries = pd.concat([all_log_analytics_queries, log_analytics_queries])

    return all_log_analytics_queries

def get_available_scope(queries, configuration):
    current_mode = configuration["mode"]
    return execute_azure_log_analytics_query(queries["azure_log_analytics"][current_mode]["get_scope"], configuration["log_analytics"]["workspace_id"])


# Power BI & Azure Analysis Service
def execute_dmv(df_scope, configuration, queries, query_name):
    def get_column_matching(columns):
        output = {}
        index = 0
        for column in columns:
            output[index] = column
            index += 1
        return output

    df_dmv = None

    application_id = configuration["azure"]["application_id"]
    secret_key = configuration["azure"]["secret_key"]
    tenant_name = configuration["azure"]["tenant_name"]
    tenant_id = configuration["azure"]["tenant_id"]
    
    for index in df_scope.index:
        workspace = df_scope["workspace_server"][index]
        dataset = df_scope["dataset_database"][index]

        query_config = queries["dmv"][query_name]
        query = query_config["query"].format(workspace_server = workspace, dataset_database = dataset)
        columns = query_config["columns"]

        if(configuration["mode"] == "power_bi"):
            connection_string = "Provider=MSOLAP;Data Source=powerbi://api.powerbi.com/v1.0/{tenant_name}/{workspace};catalog={dataset};User ID=app:{application_id}@{tenant_id};Password={secret_key}".format(application_id = application_id, secret_key = secret_key, tenant_name = tenant_name, tenant_id = tenant_id, workspace = workspace, dataset = dataset)
        else:
            # TODO
            exit(0)

        connection = Pyadomd(connection_string)

        try:
            connection.open()
        except:
            if(configuration["mode"] == "power_bi"):
                print_log("Connection failed to dataset '{tenant_name}/{workspace}/{dataset}' with provided SPN".format(tenant_name = tenant_name, workspace = workspace, dataset = dataset), 50)
                exit(0)
            else:
                # TODO
                exit(0)


        result = connection.cursor().execute(query)
        df_result = pd.DataFrame(result.fetchone())
        connection.close()

        df_result.rename(columns = get_column_matching(columns), inplace = True)

        if df_dmv is None:
            df_dmv = df_result
        else:
            df_dmv = pd.concat([df_dmv, df_result])

    return df_dmv

def get_model_objects(df_scope, configuration, queries):
    def get_object_name(x):
        if(x["object_name"] is None):
            return x["object_alternative_name"]
        return x["object_name"]

    df_tables = execute_dmv(df_scope, configuration, queries, "get_tables")
    df_columns = execute_dmv(df_scope, configuration, queries, "get_columns")
    df_measures = execute_dmv(df_scope, configuration, queries, "get_measures")

    df_columns["object_name"] = df_columns.apply(get_object_name, axis = 1)

    df_columns = pd.merge(df_columns, df_tables, on = ["workspace_server", "dataset_database", "table_id"], how = "inner")
    df_measures = pd.merge(df_measures, df_tables, on = ["workspace_server", "dataset_database", "table_id"], how = "inner")

    df_output = pd.concat([df_columns, df_measures])

    df_output["table_name_raw"] = df_output["table_name"]
    df_output["object_name_raw"] = df_output["object_name"]

    df_output["table_name"] = df_output["table_name"].str.upper()
    df_output["object_name"] = df_output["object_name"].str.upper()

    return df_output

def get_storage(df_scope, df_objects, configuration, queries):
    def get_id_from_string(x):
        column = "object_id"
        if "table_id" in x.index.tolist() and re.match("^\S{1}\$", x["table_id"]):
            column = "table_id"

        matches = re.findall("\(([0-9]*)\)$", x[column])
        if len(matches) == 0:
            return 0
        else:
            return int(matches[0])

    df_storage_dictionary = execute_dmv(df_scope, configuration, queries, "get_storage_dictionary")
    df_storage_table_segments = execute_dmv(df_scope, configuration, queries, "get_storage_table_segments")

    df_storage_dictionary["object_id"] = df_storage_dictionary.apply(get_id_from_string, axis = 1)
    df_storage_table_segments["object_id"] = df_storage_table_segments.apply(get_id_from_string, axis = 1)
    
    df_storage_table_segments = df_storage_table_segments.drop(["table_id"], axis = 1)
    df_storage_table_segments = df_storage_table_segments.groupby(["workspace_server", "dataset_database", "object_id"], as_index = False).sum("used_size")

    df_storage = df_objects.merge(df_storage_dictionary, on = ["workspace_server", "dataset_database", "object_id"], how = "left")
    df_storage = df_storage.merge(df_storage_table_segments, on = ["workspace_server", "dataset_database", "object_id"], how = "left")
    
    df_storage["object_type"] = "COLUMN"

    df_storage = df_storage[["workspace_server", "dataset_database", "object_type", "table_name_raw", "object_name_raw", "dictionary_size", "used_size"]]
    
    return df_storage