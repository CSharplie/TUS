import os
import pandas as pd

def get_compression_options(name):
    return dict(method = "zip", archive_name = "{name}.json".format(name = name))  

def get_cache_path(name):
    return "cache/{name}.zip".format(name = name)

def cache_is_available(name):
    file_path = get_cache_path(name)
    return os.path.isfile(file_path)

def save_cache(df, name, cache_configuration):
    if(cache_configuration["enabled"]):
        file_path = get_cache_path(name)
        compression_options = get_compression_options(name)
        df.to_json(file_path, orient = "table", compression = compression_options)

def open_cache(name): 
    file_path = get_cache_path(name)
    compression_options = get_compression_options(name)
    return pd.read_json(file_path, orient = "table", compression = compression_options)
