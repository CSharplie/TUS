import logging

from core.cache import cache_is_available, open_cache, save_cache

DEBUG_LEVEL_PRINT = 25 
logging.addLevelName(25, "INFO")
def logging_print(self, message, *args, **kws):
    self._log(DEBUG_LEVEL_PRINT, message, args, **kws) 

logging.Logger.print = logging_print
logging.basicConfig(format = "[%(asctime)s] [%(levelname)s] [%(message)s]", datefmt = "%Y-%m-%d %H:%M:%S", level = 25)

def print_log(message, level = DEBUG_LEVEL_PRINT):
    l = logging.getLogger()
    l.setLevel(level)
    l.log(level, message)

def process_or_get_from_cache(function, cache_configuration, cache_name, cache_parameter, **args):
    df_output = None
    if cache_is_available(cache_name) and cache_configuration[cache_parameter]:
        print_log("Get data from cache")
        df_output = open_cache(cache_name)
    else:
        print_log("Start operation")
        df_output = function(**args)
        if(df_output is None):
            print_log("Operation done with error", 40)
            print_log("The dataframe is empty. The cache can't be saved", 40)
        else:
            save_cache(df_output, cache_name, cache_configuration)
            print_log("Operation done")
    return df_output