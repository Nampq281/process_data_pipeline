import json
import ast
import pandas as pd
import polars as pl
import numpy as np
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from math import ceil

def month_diff(start, end):
  try:
      return ceil((end - start).days / 30.5)
  except:
      return np.nan    
      
def ym_format(input_str, fmt):
    try: 
        output_dt = dt.strptime(str(input_str), fmt)
    except:
        output_dt = np.nan
    return output_dt


def create_ym_format(df, ym_col, fmt='%Y%m'):
    return df[ym_col].apply(lambda row: ym_format(row, fmt))





# -------------------------------PROCSES PCB----------------------------------
from pip._vendor.rich.progress import (
    Progress, SpinnerColumn, TimeElapsedColumn
)
def polars_bar(total, title="Processing", transient=False):
    bar = Progress( 
        SpinnerColumn(),
        *Progress.get_default_columns(),
        TimeElapsedColumn(),
        transient=transient # remove bar when finished
    )
    def _run(func, *args, **kwargs):
        task_id = bar.add_task(title, total=total)
        def _execute(*args, **kwargs):
            bar.update(task_id, advance=1)
            return func(*args, **kwargs)
        return lambda self: _execute(self, *args, **kwargs)
    bar.run = _run
    return bar

def explode_nest(row, result:list, field:str, id_col_list:list, cus_level=True):
    if cus_level: # Parse level 1: Chuyển sang dạng Literal(dict) vì cell đang dạng string
        try:
            fmt = ast.literal_eval(row[field])
        except:
            fmt = {}  
    else: # Parse level 2: Đã đang ở dạng pl.List(Struct()) dtype
        fmt = row[field]

    # Tập trung vào 1 list vì dạng cấu trúc nested thông tin bên PCB trả ra
    if not isinstance(fmt, list):
        item_list = [fmt]
    else:
        item_list = fmt
        
    flat_lst = [] # Append thông tin được duỗi ra để sau concat
    for item in item_list:
        if item!=None:
            flat_lst+=[pl.json_normalize(item, strict=False)] # Duỗi ra dạng DataFrame
        else:
            flat_lst+=[pl.DataFrame()] # Để rỗng nếu không có thông tin
    
    # Attach thêm trường id để tạo các bảng quan hệ
    for i in range(len(flat_lst)):
        for id_col in id_col_list:
            flat_lst[i] = flat_lst[i].with_columns(pl.lit(row[id_col]).alias(id_col))
    result += flat_lst   

def unpack_data(df:pl.DataFrame, field:list, id_col_list:list, cus_level=True) -> pl.DataFrame:
    df_holder = []
    with polars_bar(total=df.height) as bar:
        df.with_columns(
            pl.struct(field+id_col_list)
                .map_elements(bar.run(explode_nest, 
                                      result=df_holder, 
                                      field=field[0], 
                                      id_col_list=id_col_list,
                                      cus_level=cus_level)
                             )
        )
    return df_holder

    
def treat_col_before_concat(df_holder:list, treat_col:list, dtype_col:list):
    error = {'index':[], 'field':[]}
    for i in tqdm(range(len(df_holder))):
        df = df_holder[i]
        for col, dt in zip(treat_col, dtype_col):
            if col in df.columns:
                # Treat 'null' to None
                df = df.with_columns(pl.col(pl.String).replace({'null':None}))
                try:
                    # Cast dtype
                    df = df.with_columns(pl.col(col).cast(dt))
                except:
                    error['index'].append(i)
                    error['field'].append(col)
            else:
                pass
        df_holder[i] = df
    return df_holder, error
    