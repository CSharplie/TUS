import yaml
from sys import path

path.append("resources")

from core.miscellaneous import print_log, process_or_get_from_cache
from core.ingestion import get_available_scope, get_log_analytics_raw_queries, get_model_objects, get_storage, set_auth_environment_variables
from core.processing import get_model_dependencies, get_parsed_queries
from core.export import export_output, get_output_storage, get_output_objects, get_output_usage_by_object

with open("queries.yml") as f:
    queries = yaml.load(f, Loader = yaml.loader.SafeLoader)

with open("settings.yml") as f:
    configuration = yaml.load(f, Loader = yaml.loader.SafeLoader)

if configuration["mode"] != "aas":
    configuration["mode"] = "power_bi"

print_log("Start auditing")
set_auth_environment_variables(configuration)

print_log("Get raw queries from Azure Log Analytics")
df_raw_queries = process_or_get_from_cache(get_log_analytics_raw_queries, configuration["cache"], "queries_raw", "use_raw_queries_cache", configuration = configuration, queries = queries)

print_log("Get available scope")
df_scope = process_or_get_from_cache(get_available_scope, configuration["cache"], "available_scope", "use_scope_cache", configuration = configuration, queries = queries)

print_log("Get models objects")
df_objects = process_or_get_from_cache(get_model_objects, configuration["cache"], "model_objects", "use_model_cache", configuration = configuration, queries = queries, df_scope = df_scope)

print_log("Get storage information")
df_storage =  process_or_get_from_cache(get_storage, configuration["cache"], "storage", "use_storage_cache", configuration = configuration, queries = queries, df_scope = df_scope, df_objects = df_objects)

print_log("Calculate models dependencies")
df_dependencies = process_or_get_from_cache(get_model_dependencies, configuration["cache"], "model_dependencies", "use_model_cache", df_objects = df_objects)

print_log("Parse queries")
df_parsed_queries = process_or_get_from_cache(get_parsed_queries, configuration["cache"], "queries_parsed", "use_parsed_queries_cache", df_raw_queries = df_raw_queries, df_objects = df_objects)

print_log("Generate output datasets")
print_log("Usage by object level")
df_output_usage_by_object = get_output_usage_by_object(df_parsed_queries, df_objects, df_dependencies)

print_log("Export results")
df_output_objects = get_output_objects(df_objects)
df_output_storage = get_output_storage(df_storage, df_parsed_queries)

export_output(df_output_objects, "objects", False)
export_output(df_output_storage, "storage", True)
export_output(df_output_usage_by_object, "usage_by_objects", True)

exit(0)