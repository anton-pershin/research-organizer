import inspect
from functools import partial
import os
import os.path
import shutil
import re
import numpy as np

def create_file_mkdir(filepath):
    """Opens a filepath in a write mode (i.e., creates/overwrites it). If the path does not exists,
    subsequent directories will be created.
    """
    dirpath = os.path.dirname(filepath)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    return open(filepath, 'w')


def get_templates_path():
    """Returns the absolute path to templates directory. It is useful when the module is imported from elsewhere.
    """
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')

def append_code(obj, obj_funcs, code_appendix):
    """Adds the code defined by the function code_appendix in the end of the method obj_funcs of the object obj.
    """
    def extended_func(func, *args, **kwds):
        func(*args, **kwds)
        code_appendix(*args, **kwds)

    for func_name in obj_funcs:
        func = getattr(obj, func_name)
        if not func:
            raise Exception('Function {} not found'.format(func_name))
        setattr(obj, func_name, partial(extended_func, func))

def is_sequence(arg):
    """Checks whether arg is a sequence (string does not count as a sequence)
    """
    return (not hasattr(arg, "strip") and
            hasattr(arg, "__getitem__") or
            hasattr(arg, "__iter__"))

def do_atomic(proc_func, cleanup_func):
    """Executes the function proc_func such that if an expection is raised, the function cleanup_func
    is executes and only after that the expection is hand over further. It is useful when proc_func
    creates something which should be removed in the case of emergency.
    """
    try:
        proc_func()
    except Exception as err:
        cleanup_func()
        raise err

def make_atomic(proc_func, cleanup_func):
    """Returns a function corresponding to do_atomic() to which proc_func and cleanup_func are passed.
    """
    return partial(do_atomic, proc_func, cleanup_func)

def cp(from_, to_):
    """
    Copy from_ to to_ where from_ may be file or dir and to_ is a dir.
    """
    if os.path.isfile(from_):
        shutil.copy(from_, to_)
    else:
        shutil.copytree(from_, to_)

def rm(target):
    """Remove target which may be file or dir.
    """
    if os.path.isfile(target):
        os.remove(target)
    else:
        shutil.rmtree(target)

def merge_dicts(*dict_args):
    """Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    Source: Aaron Hall, https://stackoverflow.com/questions/38987/how-to-merge-two-dictionaries-in-a-single-expression
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result

def find_dir_by_named_regexp(regexp, where):
    """Search for dir in where which satisfies regexp. If successful, parses the dir according to named regexp.
    Returns a tuple (found_dir, params_from_named_regexp) or None if not found.
    """
    dirnames = next(os.walk(where))[1]
    for dir_ in dirnames:
        parsing_params = parse_by_named_regexp(regexp, dir_)
        if parsing_params is not None:
            return dir_, parsing_params
    return None

def parse_by_named_regexp(regexp, val):
    """Parses val according to named regexp. Return a dictionary of params.
    """
    matching = re.search(regexp, val)
    if matching is None:
        return None
    return matching.groupdict()

def parse_datafile(path, data_names, transform_funcs, cols_to_parse=[]):
    """Parses a data file given by path and structured as a table where rows are separated by \n
    and columns are separated by any of whitespaces. The first line in the file will be ignored.
    Processed columns are given by cols_to_parse (all columns will be processed if it is empty).
    Corresponding names and transformation functions for columns in cols_to_parse are given by 
    data_names and transform_funcs. Transformation function must be a mapping string -> type.
    
    Returns a dictionary where a key corresponds to a column name (i.e., taken from data_names)
    and a value corresponds to a list of the columns values taken from all rows.
    """
    if cols_to_parse == []:
        cols_to_parse = range(len(data_names))
    if len(data_names) != len(transform_funcs) or len(data_names) != len(cols_to_parse):
        raise Exception('Number of data names, transform functions and columns to be parsed is inconsistent')
    data = {}
    for data_name in data_names:
        data[data_name] = []

    f = open(path, 'r') # if not found, expection will be raised anyway
    lines = f.readlines()
    for line in lines[1:]: # skip the first line
        tmp = line.split()
        if len(tmp) < len(data_names):
            raise Exception('Number of given data names is larger than number of columns we have in the data file.')
        for i, data_name in enumerate(data_names):
            val = tmp[cols_to_parse[i]]
            data[data_name].append(transform_funcs[i](val))
    return data

def parse_timed_numdatafile(path):
    """Parses a data file given by path and structured as a table where rows are separated by \n
    and columns are separated by any of whitespaces. The table here has an interpretation of a matrix whose 
    rows axis corresponds to time axis and columns axis corresponds to data axis. Moreover, the first column
    contains the time values so the data is contained in columns starting from the second one.

    Returns time_list (a list of times from the first column) and data_matrix (a list of numpy arrays of data where
    list's index corresponds to the time index). 
    """
    time = []
    data = []
    f = open(path, 'r') # if not found, expection will be raised anyway
    lines = f.readlines()
    for line in lines[1:]: # skip the first line
        tmp = line.split()
        time.append(float(tmp[0]))
        timed_data = np.zeros((len(tmp) - 1, ))
        for i, val in enumerate(tmp[1:]):
            timed_data[i] = float(val)
        data.append(timed_data)
    return time, data
