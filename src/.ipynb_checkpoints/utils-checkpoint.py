import json
import ast
import pandas as pd
import numpy as np
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

def to_json_fmt(node):
    try:
        fmt = ast.literal_eval(node)
    except:
        fmt = {}
    return fmt

def flatten(node:list):
    '''
    [nested dict] into multi rows dataframe
    params node: list of nested dict
    output flat: list of pd.DataFrame
    '''
    item_list = to_json_fmt(node)
    if not isinstance(item_list, list):
        item_list = [item_list]
        
    #Split multiple dict into smaller dict. 
    #Example: CommonData -> each contract
    flat_lst = [pd.json_normalize(item) for item in item_list]
    return flat_lst

def attach_id(id_col_list, df_parent, flat_lst):
    '''
    flat_lst list: list of dataframe
    id_col list: assign name of id column
    df_parent pd.DataFrame: parent node
    '''
    for table in flat_lst:
        for id_col in id_col_list:
            table[id_col] = df_parent[id_col]
    return flat_lst

def explode_nest(result:list, id_col_list:list, df_parent:pd.DataFrame, node):
    flat_lst = flatten(node)
    flat_lst_attached = attach_id(id_col_list, df_parent, flat_lst)
    result += flat_lst_attached    



from datetime import datetime as dt
from dateutil.relativedelta import relativedelta

def ym_format(input_str, fmt):
    try: 
        output_dt = dt.strptime(str(input_str), fmt)
    except:
        output_dt = np.nan
    return output_dt


def create_ym_format(df, ym_col, fmt='%Y%m'):
    return df[ym_col].apply(lambda row: ym_format(row, fmt))


from math import ceil
def month_diff(start, end):
  try:
      return ceil((end - start).days / 30.5)
  except:
      return np.nan      



      

# def month_diff(end, start):
#     """
#     end, start: datetime[64] format
#     """
#     try:
#         return relativedelta(end, start).months+1
#     except:
#         return np.nan  

# def gen_dict_extract(key, var):
#     """
#     find all values from occurence of key in a nested dict
#     """
#     result = []
#     if hasattr(var,'items'):
#         for k, v in var.items():
#             if k == key:
#                 result.append(v)
#             if isinstance(v, dict):
#                 for result2 in gen_dict_extract(key, v):
#                     result.append(result2)
#             elif isinstance(v, list):
#                 for d in v:
#                     for result2 in gen_dict_extract(key, d):
#                         result.append(result2)    