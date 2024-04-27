import ipywidgets as widgets
import pandas as pd
import numpy as np
import os
import sys
from ipywidgets import Layout, Box, Label

# from utils_io import logger
from utils_io import Logger

logger = Logger().logger
logger.propagate = False

if len(logger.handlers) > 1:
    for handler in logger.handlers:
        logger.removeHandler(handler)
    del logger
    logger = Logger().logger
    logger.propagate = False


class FormsPatternSearch:

    def __init__(self, data_source_dir):
        self.data_source_dir = data_source_dir
        self.form_01_to_null()
        self.fn_01 = None
        self.fn_02 = None

    def form_01_to_null(self):
        self.sheets_01 = []
        self.sheets_02 = []

        self.selected_sheet_01 = None
        self.selected_sheet_02 = None
        self.fn_check_file_01_drop_down = None
        self.fn_check_file_02_drop_down = None
        self.check_sheet_names_01_drop_down = None
        self.check_sheet_names_02_drop_down = None
        self.form_01 = None

    def form_param_01(self, fn_list):

        form_item_layout = Layout(display='flex', flex_flow='row', justify_content='space-between')

        self.fn_check_file_01_drop_down = widgets.Dropdown( options=fn_list, value=None)
        check_box_file_01 = Box([Label(value="Выберите Excel-файл с Характеристиками из Наименования"), self.fn_check_file_01_drop_down], layout=form_item_layout)
        # multi_select = Box([Label(value="Выберите разделы (Ctrl для мнж выбора) для сравнения: 'Услуги', 'ЛП', 'РМ':"), self.sections_drop_douwn], layout=form_item_layout) #, tips='&&&')

        self.check_sheet_names_01_drop_down = widgets.Dropdown(value=None)
        check_box_sheet_names_01 = Box([Label(value="Выберите Лист Excel с Характеристиками из Наименования"), self.check_sheet_names_01_drop_down], layout=form_item_layout)

        self.fn_check_file_02_drop_down = widgets.Dropdown( options=fn_list, value=None)
        check_box_file_02 = Box([Label(value="Выберите Excel-файл с Характеристиками УМО ЕМИАС"), self.fn_check_file_02_drop_down], layout=form_item_layout)
        # multi_select = Box([Label(value="Выберите разделы (Ctrl для мнж выбора) для сравнения: 'Услуги', 'ЛП', 'РМ':"), self.sections_drop_douwn], layout=form_item_layout) #, tips='&&&')

        self.check_sheet_names_02_drop_down = widgets.Dropdown(value=None)
        check_box_sheet_names_02 = Box([Label(value="Выберите Лист Excel с Характеристиками УМО ЕМИАС"), self.check_sheet_names_02_drop_down], layout=form_item_layout)

        form_items = [check_box_file_01, check_box_sheet_names_01, 
                      check_box_file_02, check_box_sheet_names_02,]

        self.form_01 = Box(form_items, layout=Layout(display='flex', flex_flow= 'column', border='solid 2px', align_items='stretch', width='75%')) #width='auto'))
        # return self.form_01, fn_check_file1_drop_douwn, fn_check_file2_drop_douwn, sections_drop_douwn

    def on_fn_check_file_01_drop_douwn_change(self, change):
        self.fn_01 = self.fn_check_file_01_drop_down.value
        
        xl_01 = pd.ExcelFile(os.path.join(self.data_source_dir, self.fn_01))
        self.sheets_01 = xl_01.sheet_names
        print(f"Листы файла с Характеристиками из Наименования: {str(self.sheets_01)}") # logger
        self.check_sheet_names_01_drop_down.options = self.sheets_01

    def on_fn_check_file_02_drop_douwn_change(self, change):
        self.fn_02 = self.fn_check_file_02_drop_down.value
        
        xl_02 = pd.ExcelFile(os.path.join(self.data_source_dir, self.fn_02))
        self.sheets_02 = xl_02.sheet_names
        print(f"Листы файла с Характеристиками УМО ЕМИАС: {str(self.sheets_02)}") # logger
        self.check_sheet_names_02_drop_down.options = self.sheets_02


  
