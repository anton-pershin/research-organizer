import inspect
from functools import partial
import os
import os.path
import shutil
import re

def append_code(obj, obj_funcs, code_appendix):
    def extended_func(func, *args, **kwds):
        func(*args, **kwds)
        code_appendix(*args, **kwds)

    for func_name in obj_funcs:
        func = getattr(obj, func_name)
        if not func:
            raise Exception('Function {} not found'.format(func_name))
        setattr(obj, func_name, partial(extended_func, func))

def is_sequence(arg):
    return (not hasattr(arg, "strip") and
            hasattr(arg, "__getitem__") or
            hasattr(arg, "__iter__"))

def do_atomic(proc_func, cleanup_func):
    try:
        proc_func()
    except Exception as err:
        cleanup_func()
        raise err

def make_atomic(proc_func, cleanup_func):
    return partial(do_atomic, proc_func, cleanup_func)

def cp(from_, to_):
    if os.path.isfile(from_):
        shutil.copy(from_, to_)
    else:
        shutil.copytree(from_, to_)

def rm(target):
    if os.path.isfile(target):
        os.remove(target)
    else:
        shutil.rmtree(target)

def merge_dicts(*dict_args):
    """
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    Source: Aaron Hall, https://stackoverflow.com/questions/38987/how-to-merge-two-dictionaries-in-a-single-expression
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result

def find_dir_by_named_regexp(where, regexp):
    dirnames = next(os.walk(where))[1]
    for dir_ in dirnames:
        parsing_params = parse_by_named_regexp(regexp, dir_)
        if parsing_params is not None:
            return parsing_params
    return None

def parse_by_named_regexp(regexp, val):
    matching = re.search(regexp, val)
    if matching is None:
        return None
    return matching.groupdict()
