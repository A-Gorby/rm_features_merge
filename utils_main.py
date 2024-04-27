import pandas as pd
import numpy as np
import os, sys, glob
import humanize
import re
import regex
import xlrd

import json
import itertools
import collections
#from urllib.request import urlopen
#import requests, xmltodict
import time, datetime
import math
from pprint import pprint
import gc
from tqdm import tqdm
tqdm.pandas()
import pickle

import duckdb
import pyarrow

import logging
import zipfile
import tarfile
import argparse

import warnings
warnings.filterwarnings("ignore")


from utils_io import get_humanize_filesize
from utils_io import Logger

logger = Logger().logger
logger.propagate = False

if len(logger.handlers) > 1:
    for handler in logger.handlers:
        logger.removeHandler(handler)
    # del logger
    # logger = Logger().logger
    # logger.propagate = False

def read_data(data_source_dir,
    fn_features_name,
    sh_n_features_name,
    fn_features_pre,
    sh_n_features_pre,
    ):
    if fn_features_name is None or sh_n_features_name is None or fn_features_pre is None or sh_n_features_pre is None:
        logger.info("Не опеределены все или один из входных фалов и листов Excel")
        logger.info(f"Файл Excel с Характеристиками из Наименования: '{fn_features_name}'")
        logger.info(f"Лист Excel с Характеристиками из Наименования: '{sh_n_features_name}'")
        logger.info(f"Файл Excel с Характеристиками УМО ЕМИАС: '{fn_features_pre}'")
        logger.info(f"Лист Excel с Характеристиками УМО ЕМИАС: '{sh_n_features_pre}'")
    if not os.path.exists(os.path.join(data_source_dir, fn_features_name)):
        logger.error(f"Файл Excel с Характеристиками из Наименования: '{fn_features_name}' не найден")
    if not os.path.exists(os.path.join(data_source_dir, fn_features_pre)):
        logger.error(f"Файл Excel с Характеристиками УМО ЕМИАС: '{fn_features_pre}' не найден")
    df_04_plus_separated_parts_rows_upd_01 = pd.read_excel(os.path.join(data_source_dir, fn_features_name), sheet_name=sh_n_features_name) 
    logger.info(f"Файл Excel с Характеристиками из Наименования: (строк, колонок): {str(df_04_plus_separated_parts_rows_upd_01.shape)}")
    try:
        df_rm_characteristics_02 = pd.read_excel(os.path.join(data_source_dir, fn_features_pre), sheet_name=sh_n_features_pre, converters={'ИНП':str, 'Наименование СПГЗ':str})
        logger.info(f"Файл Excel с Характеристиками УМО ЕМИАС (строк, колонок): {str(df_rm_characteristics_02.shape)}")
    except Exception as err:
       logger.error(str(err))

    req_cols_fn_features_name = ['Наименование вида', 'Код КПГЗ 4-го уровня',
       'Наименование КПГЗ 4-го уровня', 'Наименование позиции', 'Изделие',
       'Характеристика название', 'Характеристика значение']
    if not set(req_cols_fn_features_name).issubset(df_04_plus_separated_parts_rows_upd_01.columns):
        logger.error(f"В файле Excel с Характеристиками из Наименования отсутствует одна или несколько обязательных колонок: \n{str(req_cols_fn_features_name)}")

    req_cols_fn_features_pre = ['Наименование характеристики', 'Код ОКЕИ', 'Единица измерения',
       'id значения характеристики', 'Значение характеристики',
       'Условная операция', 'Обязательная характеристика',
       'Тип выбора значения', 'Тип', 'Стандартизированная характеристика',
       'Специальная характеристика', 'КТРУ характеристика', 'ИНП',
       'Наименование СПГЗ', 'Наименование КПГЗ', 'Наименование вида',
       'Наименование категории', 'Код КПГЗ нижнего уровня',
       'Характеристика из названия']
    if not set(req_cols_fn_features_pre).issubset(df_rm_characteristics_02.columns):
        logger.error(f"В файле Excel с Характеристиками УМО ЕМИАС отсутствует одна или несколько обязательных колонок: \n{str(req_cols_fn_features_pre)}")

    return df_04_plus_separated_parts_rows_upd_01, df_rm_characteristics_02

def preprocess_data(df_04_plus_separated_parts_rows_upd_01, df_rm_characteristics_02):
    
    df_04_plus_separated_parts_rows_upd_01['Наименование позиции upd'] = df_04_plus_separated_parts_rows_upd_01['Наименование позиции']
    df_04_plus_separated_parts_rows_upd_01['Наименование позиции'] = df_04_plus_separated_parts_rows_upd_01['Наименование позиции upd'].str.replace(r" +", " ",regex=True).str.strip()
    mask = df_04_plus_separated_parts_rows_upd_01['Наименование позиции'] != df_04_plus_separated_parts_rows_upd_01['Наименование позиции upd']
    logger.info(f"Обновлено строк с 'Наименование позиции' из данных с Характеристиками из Наименования: {df_04_plus_separated_parts_rows_upd_01[mask].shape[0]}/{df_04_plus_separated_parts_rows_upd_01.shape[0]}")
    df_04_plus_separated_parts_rows_upd_01.drop(columns=['Наименование позиции upd'], inplace=True)

    df_rm_characteristics_02['Наименование характеристики upd'] = df_rm_characteristics_02['Наименование характеристики'].str.strip()
    df_rm_characteristics_02['Наименование характеристики upd'] = df_rm_characteristics_02['Наименование характеристики upd'].progress_apply(
            lambda x: re.sub(r" +", " ", x) # сократить до одного двойные пробелы
    )
    df_rm_characteristics_02['Наименование характеристики upd'] = df_rm_characteristics_02['Наименование характеристики upd'].progress_apply(
            lambda x: x[:-1] if x.endswith('.') else x
    )
    mask = df_rm_characteristics_02['Наименование характеристики upd']!=df_rm_characteristics_02['Наименование характеристики']
    logger.info(f"Обновлено строк с 'Наименование характеристики' из данных УМО ЕМИАС: {df_rm_characteristics_02[mask].shape[0]}/{df_rm_characteristics_02.shape[0]}")

    df_rm_characteristics_02['Значение характеристики upd'] = df_rm_characteristics_02['Значение характеристики'].str.strip()
    df_rm_characteristics_02['Значение характеристики upd'] = df_rm_characteristics_02['Значение характеристики upd'].progress_apply(
        lambda x: re.sub(r" +", " ", x) if type(x)==str else x # сократить до одного двойные пробелы
        )
    df_rm_characteristics_02['Значение характеристики upd'] = df_rm_characteristics_02['Значение характеристики upd'].progress_apply(
        lambda x: x[:-1] if (type(x)==str) and x.endswith('.') else x
    )
    mask = df_rm_characteristics_02['Значение характеристики upd']!=df_rm_characteristics_02['Значение характеристики']
    logger.info(f"Обновлено строк с 'Значение характеристики' из данных УМО ЕМИАС: {df_rm_characteristics_02[mask].shape[0]}/{df_rm_characteristics_02.shape[0]}")

    df_rm_characteristics_02['Наименование СПГЗ upd'] = df_rm_characteristics_02['Наименование СПГЗ']
    df_rm_characteristics_02['Наименование СПГЗ'] = df_rm_characteristics_02['Наименование СПГЗ'].str.replace(r" +", " ",regex=True).str.strip()
    mask = df_rm_characteristics_02['Наименование СПГЗ upd'] != df_rm_characteristics_02['Наименование СПГЗ']
    logger.info(f"Обновлено строк с 'Наименование СПГЗ' из данных УМО ЕМИАС: {df_rm_characteristics_02[mask].shape[0]}/{df_rm_characteristics_02.shape[0]}")
    df_rm_characteristics_02.drop(columns=['Наименование СПГЗ upd'], inplace=True)

    return df_04_plus_separated_parts_rows_upd_01, df_rm_characteristics_02

# df_04_plus_separated_parts_rows_upd_01, df_rm_characteristics_02 = preprocess_data(df_04_plus_separated_parts_rows_upd_01, df_rm_characteristics_02)

def merge_rm_features_step_01(
    df_04_plus_separated_parts_rows_upd_01,
    df_rm_characteristics_02,
    debug=False
):
    if debug: print("Исходный df_rm_characteristics_02.shape:", df_rm_characteristics_02.shape)
    query = """
    SELECT pre.*, name."Изделие" FROM df_rm_characteristics_02 pre
    LEFT JOIN
    (SELECT DISTINCT "Наименование позиции", "Изделие" FROM
    df_04_plus_separated_parts_rows_upd_01) name
    ON pre."Наименование СПГЗ" = name."Наименование позиции"
    """
    # LIMIT 100
    df_rm_characteristics_04 = duckdb.query(query).df()
    if debug: print("Выхордной df_rm_characteristics_04.shape:", df_rm_characteristics_04.shape)
    logger.info(f"Количество строк с пустым значением 'Изделие' при обновлении данных УМО ЕМИАС: {df_rm_characteristics_04[df_rm_characteristics_04['Изделие'].isnull()].shape[0]}")
    return df_rm_characteristics_04

def merge_rm_features_step_02(
    df_04_plus_separated_parts_rows_upd_01,
    df_rm_characteristics_04,
    debug=False
    ):
    
    if debug: print("Исходный df_04_plus_separated_parts_rows_upd_01.shape:", df_04_plus_separated_parts_rows_upd_01.shape)
    rm_cols = ['Характеристика из названия', 'Наименование вида',	'Наименование категории', 'Наименование КПГЗ',	'Код КПГЗ нижнего уровня', 'Наименование СПГЗ', 'ИНП', ]
    rm_cols_str = 'pre."' + '", pre."'.join(rm_cols[:-2]+rm_cols[-1:]) + '"'
    rm_cols_str_02 = '"' + '", "'.join(rm_cols) + '"'

    # 'Наименование вида',	'Наименование категории' надо исключить из выборки df_04_plus_separated_parts_rows_upd_01 поскольку там они большими буквами ,
    # а взять из df_rm_characteristics_04
    name_cols = list(df_04_plus_separated_parts_rows_upd_01.columns)
    for col in ['Наименование вида',	'Наименование категории']:
        try:
            name_cols.remove(col)
        except Exception as err:
            print(col, err)
    name_cols_str ='name."' + '", name."'.join(name_cols) + '"'
    query = f"""
    SELECT {name_cols_str}, {rm_cols_str} FROM df_04_plus_separated_parts_rows_upd_01 name
    JOIN
    (SELECT DISTINCT {rm_cols_str_02} FROM
    df_rm_characteristics_04) pre
    ON pre."Наименование СПГЗ" = name."Наименование позиции" and pre."Наименование СПГЗ" is NOT NULL

    """
    # and name."Наименование позиции" is NOT NULL
    # LIMIT 100
    df_rm_separated_parts_rows_02 = duckdb.query(query).df()
    if debug: print("Выхордной df_rm_separated_parts_rows_02.shape:", df_rm_separated_parts_rows_02.shape)

    return df_rm_separated_parts_rows_02

def sort_list_by_other_list(lst_in, lst_other, debug=False):
    lst_out = []
    if set(lst_in).issubset(lst_other): # основной вариант длина меньше и все элементы вхоного списка есть в том по чему сортируем
        order = [lst_other.index(el) for el in lst_in ]
        if debug: print(order)
        d = collections.OrderedDict(sorted(dict(zip(order, lst_in )).items()))
        lst_out = list(d.values())
    else:
        lst_out = lst_in
    return lst_out

def merge_rm_features_step_03(
    df_rm_separated_parts_rows_02,
    df_rm_characteristics_04,
    debug=False
    ):

    df_rm_characteristics_04 = df_rm_characteristics_04[[
        'Наименование характеристики', 'Код ОКЕИ', 'Единица измерения', 'id значения характеристики', 'Значение характеристики', 'Условная операция',
        'Обязательная характеристика', 'Тип выбора значения', 'Тип', 'Стандартизированная характеристика', 'Специальная характеристика', 'КТРУ характеристика',
        'Наименование вида', 'Наименование категории',
        'Код КПГЗ нижнего уровня', 'Наименование КПГЗ', 'Характеристика из названия',
        'Наименование СПГЗ', 'ИНП', 'Изделие', 'Наименование характеристики upd', 'Значение характеристики upd', ]]
    if debug: print("Исходный df_rm_characteristics_04.shape:", df_rm_characteristics_04.shape)
    if debug: print("Добавляемый df_rm_separated_parts_rows_02.shape:", df_rm_separated_parts_rows_02.shape)
    need_cols = ['Наименование вида',	'Наименование категории', 'Код КПГЗ нижнего уровня', 'Наименование КПГЗ', 'Характеристика из названия'
    ] + ['Наименование позиции', 'ИНП', 'Изделие', 'Характеристика название', 'Характеристика значение',]
    need_cols_str_02 = ', '.join(['NULL'] * (len(df_rm_characteristics_04.columns)- len(need_cols))) + ', "' + '", "'.join(need_cols) + '"'
    query = f"""
    SELECT *, 'Х-ки РМ' as "Источник" FROM df_rm_characteristics_04 pre
    UNION
    SELECT {need_cols_str_02}, 'Наименование РМ' as "Источник"
    FROM df_rm_separated_parts_rows_02 name
    """
    if debug: print(query)
    df_rm_characteristics_05 = duckdb.query(query).df()
    if debug: print("Выхордной df_rm_characteristics_05.shape:", df_rm_characteristics_05.shape)
    df_rm_characteristics_05.rename(columns={'Наименование характеристики upd': 'Наименование характеристики 02', 'Значение характеристики upd': 'Значение характеристики 02'}, inplace=True)
    df_rm_characteristics_05.sort_values(["Наименование СПГЗ", "ИНП", "Источник"], ascending=[True, True, False], inplace=True)    
    return df_rm_characteristics_05
# df_rm_characteristics_05 = merge_rm_features_step_03(df_rm_separated_parts_rows_02, df_rm_characteristics_04, debug=False)

def merge_rm_features_sub(
    df_04_plus_separated_parts_rows_upd_01,
    df_rm_characteristics_02,
    debug=False):
  

    df_rm_characteristics_04 = merge_rm_features_step_01(df_04_plus_separated_parts_rows_upd_01,df_rm_characteristics_02)
    df_rm_separated_parts_rows_02 = merge_rm_features_step_02(df_04_plus_separated_parts_rows_upd_01, df_rm_characteristics_04, debug=False)
    df_rm_characteristics_05 = merge_rm_features_step_03(df_rm_separated_parts_rows_02, df_rm_characteristics_04, debug=False)
    logger.info(f"Сводный файл с Характеристиками из Наименования и с Характеристиками УМО ЕМИАС: (строк, колонок)'{df_rm_characteristics_05.shape}'")

    return df_rm_characteristics_05

# df_rm_characteristics_05 = merge_rm_features_sub(df_04_plus_separated_parts_rows_upd_01, df_rm_characteristics_02, debug=False)   
def save_to_excel(
    df_lst, 
    data_processed_dir,
    fn_main,
    sh_n_lst,
    widths_lsts_list = [[]],
    ):
    # sh_n_02 = 'Parts'
    # sh_n_02 = 'Unique_Parts'

    offset = datetime.timezone(datetime.timedelta(hours=3))
    dt = datetime.datetime.now(offset)
    str_date = dt.strftime("%Y_%m_%d_%H%M")
    fn_save = fn_main + '_' + str_date + '.xlsx'
    with pd.ExcelWriter(os.path.join(data_processed_dir, fn_save), engine='xlsxwriter') as writer:
        workbook = writer.book
        format_float = workbook.add_format({"num_format": "# ### ##0.00"})
        format_int = workbook.add_format({"num_format": "# ### ##0"})
        header_format = workbook.add_format({'bold': True,"text_wrap": 1,"valign": "top", "align": "left",}) #'fg_color': '#D7E4BC','border': 1})

        for sh_n, data_df, cols_width  in zip(sh_n_lst, df_lst, widths_lsts_list):
            data_df.to_excel(writer, sheet_name = sh_n, float_format="%.2f", index=False) #
            worksheet = writer.sheets[sh_n]
            # print(cols_width)
            for i_w, w in enumerate(cols_width):
                worksheet.set_column(i_w, i_w, w, None)
            worksheet.autofilter(0, 0, data_df.shape[0], data_df.shape[1]-1)
    logger.info(f"Обработанный файл '{fn_save}' сохранен в папке '{data_processed_dir}'")
    # !du -h "$data_processed_dir"/"$fn_save"
    file_size = get_humanize_filesize(data_processed_dir, fn_save)
    logger.info(f"Размер файла - {file_size}")

    return fn_save

def merge_rm_features_main(data_source_dir, data_processed_dir,   
        fn_features_name, sh_n_features_name,
        fn_features_pre, sh_n_features_pre,
        debug=False):
    df_04_plus_separated_parts_rows_upd_01, df_rm_characteristics_02 = read_data(
        data_source_dir,    
        fn_features_name, sh_n_features_name,
        fn_features_pre, sh_n_features_pre,
        )
    df_04_plus_separated_parts_rows_upd_01, df_rm_characteristics_02 = preprocess_data(df_04_plus_separated_parts_rows_upd_01, df_rm_characteristics_02)
    
    df_rm_characteristics_05 = merge_rm_features_sub(df_04_plus_separated_parts_rows_upd_01, df_rm_characteristics_02, debug=False)

    fn_save = save_to_excel(
        df_lst = [df_rm_characteristics_05],
        data_processed_dir=data_processed_dir,
        fn_main = fn_features_name.split('.xlsx')[0],
        sh_n_lst = ['Х_ки_наим_я_Пре_Х_ки'],
        widths_lsts_list = [[20,7,10,10, 20, 10,10,15,10,10,10,10, 20,20,20,  20, 10, 20,10,15, 20,20,20],],
        )


    return (
        df_04_plus_separated_parts_rows_upd_01, df_rm_characteristics_02, df_rm_characteristics_05, fn_save 
    )

# df_04_plus_separated_parts_rows_upd_01, df_rm_characteristics_02, df_rm_characteristics_05, fn_save = merge_rm_features_main(
#     data_source_dir, data_processed_dir,    
#     fn_features_name = forms.fn_01, sh_n_features_name = forms.check_sheet_names_01_drop_down.value,
#     fn_features_pre = forms.fn_02, sh_n_features_pre = forms.check_sheet_names_02_drop_down.value,
#         debug=False
# )    
