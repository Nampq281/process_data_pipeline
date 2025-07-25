import pandas as pd
from datetime import datetime as dt
import xmltodict
from pip._vendor.rich.progress import (
    Progress, SpinnerColumn, TimeElapsedColumn
)
from src.utils import *
from prefect import task, flow
# from prefect.futures import wait
from prefect.task_runners import ConcurrentTaskRunner
from prefect_dask import DaskTaskRunner
from prefect.logging import get_run_logger


def conso_id(id1, id2):
    if id1 == "'":
        final_id = id2
    elif id2 == "'":
        final_id = id1
    else:
        if id1 == id2:
            final_id = id1
        else:
            final_id = id1+"_"+id2
    return final_id

def get_log_subject(row):
    full_message = xmltodict.parse(row) 
    try:
        to_dict = full_message['MGResponse']['RI_Req_Output']
        try:
            to_dict = to_dict['Subject']['Matched']
        except KeyError:
            to_dict = {}

    except KeyError:
        to_dict = full_message['MGResponse']['CI_Req_Output']
        try:
            to_dict = to_dict['Subject']['Matched']
        except KeyError:
            to_dict = {}
    return str(to_dict)


    return str(to_dict)

def get_log_credit(row):
    full_message = xmltodict.parse(row) 
    try:
        to_dict = full_message['MGResponse']['RI_Req_Output']['CreditHistory']
    except KeyError:
        to_dict = full_message['MGResponse']['CI_Req_Output']['CreditHistory']
    return str(to_dict)

@task
def parse_data(process_name, df_in, field, id_lst, p_bar=False):
    try:
        df_holder = []
        total_rows = len(df_in)
        if p_bar == True:
            with Progress(SpinnerColumn(), *Progress.get_default_columns(), TimeElapsedColumn(), transient=False) as progress:
                task = progress.add_task(f"Parsing {process_name}", total=total_rows)
                def process_row(row):    
                    explode_nest(df_holder, id_lst, row, row[field[0]])
                    progress.update(task, advance=1)
                    
                df_in[id_lst + field].apply(process_row, axis=1)
                df_out = pd.concat(df_holder)
        else:
            df_in[id_lst+field].apply(lambda row: explode_nest(df_holder, id_lst, row, row[field[0]]), axis=1)
            df_out = pd.concat(df_holder)

        df_out = df_out.replace('null', np.nan)
    except KeyError:
        df_out = pd.DataFrame()
    return df_out

def gen_id_level2(df):
    try: 
        return df['cccd'].astype(str) + '_' + df['CommonData.CBContractCode']
    except KeyError:
        return None 

def treat_dtype(df, col):
    try:
        df[col] = df[col].astype(str)
    except KeyError:
        pass


@task(name="Load raw input data")
def process_raw_input(df):
    # df.columns = ['id1', 'id2', 'name', 'address', 'log', 'process_date']
    df['process_date'] = df['process_date'].astype(str)
    df['cccd'] = df[['id1', 'id2']].apply(lambda 
                                    row: conso_id(row.id1, row.id2),
                                    axis=1
                                    )
    # df['pcb'] = df['log'].apply(lambda row: get_log_credit(row))
    df['pcb_id'] = df['log'].apply(lambda row: get_log_subject(row))
    return df

@task(name="Extract data Customer ID, contract ID")
def parse_lv0(df):
    field = ['pcb_id']
    id_col_list = ['cccd', 'process_date']
    # parse cus ID data
    df_subject_id = parse_data('subject_id', df, field, id_col_list)
    return df_subject_id


@task(name="Extract data Customer level")
def parse_lv1(df):
    field = ['pcb']
    id_col_list = ['cccd', 'process_date']
    # parse cus level data
    df_root = parse_data('root', df, field, id_col_list)

    dtype_treat_lst = ['Contract.Instalments.GrantedContract',
                    'Contract.Cards.GrantedContract',
                    'Contract.NonInstalments.GrantedContract',
                    'Contract.Instalments.NotGrantedContract',
                    'Contract.Cards.NotGrantedContract',
                    'Contract.NonInstalments.NotGrantedContract']
    for i in dtype_treat_lst:
        treat_dtype(df_root, i)
    return df_root

@task(name="Extract data Contract level")
def parse_lv2(df_root):
    id_col_list = ['cccd', 'process_date']
    # parse contract level data
    contract_level_noninstall = parse_data.submit('nonins', df_root, ['Contract.NonInstalments.GrantedContract'], id_col_list)
    contract_level_install    = parse_data.submit('ins', df_root, ['Contract.Instalments.GrantedContract'], id_col_list)
    contract_level_card       = parse_data.submit('card', df_root, ['Contract.Cards.GrantedContract'], id_col_list)

    not_grant_install    = parse_data.submit('ng_ins', df_root, ['Contract.Instalments.NotGrantedContract'], id_col_list)
    not_grant_noninstall = parse_data.submit('ng_nonins', df_root, ['Contract.NonInstalments.NotGrantedContract'], id_col_list)
    not_grant_card       = parse_data.submit('ng_card', df_root, ['Contract.Cards.NotGrantedContract'], id_col_list)

    return (contract_level_noninstall, contract_level_install, contract_level_card, 
            not_grant_install, not_grant_noninstall, not_grant_card)

@task(name="Process Contract level data")
def process_contract_level(contract_level_noninstall, contract_level_install, contract_level_card):
    contract_level_noninstall['loan_code_lv2'] = gen_id_level2(contract_level_noninstall)
    contract_level_install['loan_code_lv2'] = gen_id_level2(contract_level_install)
    contract_level_card['loan_code_lv2'] = gen_id_level2(contract_level_card)

    treat_dtype(contract_level_install, 'Profiles')
    treat_dtype(contract_level_install, 'InstGuarantees')
    treat_dtype(contract_level_card, 'Profiles')
    treat_dtype(contract_level_card, 'CardsGuarantees')
    treat_dtype(contract_level_noninstall, 'Profiles')

    contract_level_card = contract_level_card[~contract_level_card['loan_code_lv2'].isna()]
    contract_level_noninstall = contract_level_noninstall[~contract_level_noninstall['loan_code_lv2'].isna()]
    contract_level_install = contract_level_install[~contract_level_install['loan_code_lv2'].isna()]  
    return (contract_level_noninstall, contract_level_install, contract_level_card)

@task(name="Extract Time series data level")
def parse_lv3(contract_level_noninstall, contract_level_install, contract_level_card):
    field = ['Profiles']
    id_col_list = ['loan_code_lv2', 'cccd', 'CommonData.CBContractCode', 'process_date']

    ts_card       = parse_data.submit('ts_card', contract_level_card, field, id_col_list)
    ts_install    = parse_data.submit('ts_ins', contract_level_install, field, id_col_list)
    ts_noninstall = parse_data.submit('ts_nonins', contract_level_noninstall, field, id_col_list)
    return (ts_card, ts_install, ts_noninstall)


@task(name="Export file")
def export_file(df_in, file_name):
    df_in.to_parquet('Output/' + file_name , index=False)

@flow(name="PCB Pipeline", task_runner=DaskTaskRunner())
def pcb_pipeline():
    logger = get_run_logger()
    chunksize = 20000
    start_chunk = 0    
    skip_rows = start_chunk * chunksize
    file_list = [
        'pcb_2022_T01_T06.csv',
        'pcb_2022_T07_T12.csv', 
        'pcb_2023_T01_T06.csv', 
        'pcb_2023_T07_T12.csv', 
        'pcb_2024_T01_T06.csv', 
        'pcb_2024_T07_T12.csv'
    ]

    for input_file in file_list:
        logger.info(f" ------------------------------------> Processing file: {input_file}")
        for i, df in enumerate(pd.read_csv(input_file, 
                                   chunksize=chunksize, 
                                #    skiprows=range(1, skip_rows + 1), # Continue from last chunk
                                   header=None, 
                                   names=['id1', 'id2', 'name', 'address', 'log', 'process_date']
                                ), start=start_chunk):
            logger.info(f"___________________________Chunk number {i}___________________________")
            # Load
            df = process_raw_input(df)

            # Parse cus ID level
            df_subject_id = parse_lv0(df)

            df_subject_id.to_parquet('Output/pcb_root_id_' + input_file.replace('.csv', f'_{i}.parquet'), index=False)

            # # Parse cus
            # df_root = parse_lv1(df)
            # # Parse contract
            # contract_level_noninstall, contract_level_install, contract_level_card, \
            #     not_grant_install, not_grant_noninstall, not_grant_card = parse_lv2(df_root)
            
            # contract_level_noninstall, contract_level_install, contract_level_card = process_contract_level(contract_level_noninstall, 
            #                                                                                                 contract_level_install, 
            #                                                                                                 contract_level_card)
            # # Parse time series
            # ts_card, ts_install, ts_noninstall = parse_lv3(contract_level_noninstall, 
            #                                                 contract_level_install, 
            #                                                 contract_level_card)
            
            # # Store results
            # result = [df_subject_id,
            #         df_root,
            #         contract_level_noninstall, 
            #         contract_level_install, 
            #         contract_level_card,
            #         not_grant_install, 
            #         not_grant_noninstall, 
            #         not_grant_card,
            #         ts_card, 
            #         ts_install, 
            #         ts_noninstall
            #         ]
            # result_file = ['subject_id',
            #             'root', 
            #             'noninstall', 
            #             'install', 
            #             'card',
            #             'not_grant_install', 
            #             'not_grant_noninstall', 
            #             'not_grant_card',
            #             'ts_card', 
            #             'ts_install', 
            #             'ts_noninstall'] 

            # output_path = [file+f'_{i}_'+input_file.replace('.csv', '.parquet') for file in result_file]

            # for x, y in zip(result, output_path):
            #         export_file(x, y)
                    
        logger.info(f"Finished file: {input_file}")        

if __name__ == "__main__":        
    pcb_pipeline()
    print("PCB Pipeline completed successfully.")