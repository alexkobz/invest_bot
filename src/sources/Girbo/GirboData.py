from __future__ import annotations

import os
from collections import defaultdict
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium import webdriver
from selenium.common import WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.ie.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text
from time import sleep
from typing import List, Dict

from api_model_python import get_project_root, Path

from src.logger.Logger import Logger


logger = Logger()


class GirboData:

    _download_dir: str = '/opt/airflow/data/girbo_fundamentals'

    def __init__(self):
        env_path: Path = get_project_root()
        load_dotenv(env_path)
        DATABASE_URI: str = (
            f"postgresql://"
            f"{os.environ['POSTGRES_USER']}:"
            f"{os.environ['POSTGRES_PASSWORD']}@"
            f"{os.environ['POSTGRES_HOST']}:"
            f"{os.environ['POSTGRES_PORT']}/"
            f"{os.environ['POSTGRES_DATABASE']}")
        self.engine = create_engine(DATABASE_URI)
        self.options = webdriver.FirefoxOptions()
        self.options.set_preference("pdfjs.disabled", True)
        self.options.set_preference("browser.download.folderList", 2)
        self.options.set_preference("browser.download.manager.useWindow", False)
        self.options.set_preference("browser.download.dir", GirboData._download_dir)
        self.options.set_preference(
            "browser.helperApps.neverAsk.saveToDisk",
            "application/pdf, application/gzip, application/force-download, text/csv")
        self.options.add_argument("--headless")
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')


    def get_organizations_cards(self):
        def request(url: str) -> None:
            try:
                driver.get(url)
            except WebDriverException as e:
                sleep(60)
                logger.exception(f'{inn} WebDriverException')
                request(url)

        table_name: str = 'api_girbo_organizations_cards'
        inns: List[str] = (
            pd.read_sql(
                """
                SELECT DISTINCT inn
                FROM public_marts.dim_moex_securities
                """
                , self.engine
            )['inn']
            .to_list()
        )
        self.engine.execute(
            sa_text(f'''TRUNCATE TABLE {table_name}''').execution_options(autocommit=True))
        l = len(inns)
        for inn in inns:
            l -= 1
            driver = webdriver.Remote(
                command_executor="http://host.docker.internal:4444/wd/hub",
                options=self.options)
            url = f"https://bo.nalog.ru/search?allFieldsMatch=false&inn={inn}"
            request(url)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            link = soup.find("a", class_="results-search-table-row")
            try:
                organizations_card = link.get("href")
                card = organizations_card.replace('/organizations-card/', '')
                pd.DataFrame(
                    data={'inn': [inn], 'card': [card]}
                ).to_sql(
                    name=table_name,
                    con=self.engine,
                    if_exists='append',
                    index=False)
                logger.info(f'{inn} success\n{l} inns left')
            except AttributeError:
                pd.DataFrame(
                    data={'inn': [inn], 'card': ['']}
                ).to_sql(
                    name=table_name,
                    con=self.engine,
                    if_exists='append',
                    index=False)
                logger.exception(f'{inn} not found')
            except Exception as e:
                logger.exception(f'{inn} EXCEPTION {e}')
            finally:
                driver.quit()
            sleep(1)

    def download_fundamentals(self):

        def request(url: str) -> None:
            try:
                driver.get(url)
            except WebDriverException as e:
                sleep(60)
                logger.exception(f'{card} WebDriverException')
                request(url)

        def __click_button(xpath_value: str) -> bool:
            try:
                button: WebElement = driver.find_element(
                    By.XPATH,
                    value=xpath_value)
            except:
                logger.info('Button not found')
                return False
            if button.is_displayed() and button.is_enabled():
                button.click()
                return True
            return False

        button_xpath: Dict[str, str] = {
            'popup_close_button': '//*[@id="short-info"]/div[2]/button',
            'report_button': '/html/body/div[1]/main/div[2]/div[1]/div/div/div[2]/div/div[1]/div[2]/div/button',
            'year_button': "/html/body/div[1]/main/div[2]/div[1]/div/div/div[2]/div/div[1]/div[2]/div/div/div/div[1]/div[2]/div/button",
            'balance_checkbox': "//div[@class='form-item-checkbox_checkbox' and @data-option='balance']",
            'financial_result_checkbox': "//div[@class='form-item-checkbox_checkbox' and @data-option='financialResult']",
            'capital_change_checkbox': "//div[@class='form-item-checkbox_checkbox' and @data-option='capitalChange']",
            'funds_movement_checkbox': "//div[@class='form-item-checkbox_checkbox' and @data-option='fundsMovement']",
            'targeted_funds_using_checkbox': "//div[@class='form-item-checkbox_checkbox' and @data-option='targetedFundsUsing']",
            'download_button': '/html/body/div[1]/main/div[2]/div[1]/div/div/div[2]/div/div[1]/div[2]/div/div/div/div[3]/button',
        }

        os.chdir(GirboData._download_dir)
        cards: List[str] = (
            pd.read_sql(
                """
                SELECT DISTINCT card
                FROM public_marts.dim_girbo_organizations_cards
                WHERE coalesce(card, '') != ''
                """
                , self.engine
            )['card']
            .to_list()
        )
        for card in cards:
            driver: WebDriver = webdriver.Remote(
                command_executor="http://host.docker.internal:4444/wd/hub",
                options=self.options)
            url = f"https://bo.nalog.ru/organizations-card/{card}"
            request(url)
            __click_button(button_xpath['popup_close_button'])
            if __click_button(button_xpath['report_button']):
                for year_button in driver.find_elements(
                        By.XPATH,
                        value=button_xpath['year_button']):
                    if year_button.is_displayed() and year_button.is_enabled():
                        year_button.click()
                    __click_button(button_xpath['balance_checkbox'])
                    __click_button(button_xpath['financial_result_checkbox'])
                    __click_button(button_xpath['capital_change_checkbox'])
                    __click_button(button_xpath['funds_movement_checkbox'])
                    __click_button(button_xpath['targeted_funds_using_checkbox'])
                    __click_button(button_xpath['download_button'])
            driver.quit()
            logger.info(f"{card} success")
            sleep(1)

    # TODO refactor function
    def parse_fundamentals(self):

        import zipfile

        balance_columns = ['1110', '1120', '1130', '1140', '1150', '1160', '1170', '1180', '1190', '1100', '1210',
                           '1220', '1230', '1240', '1250', '1260', '1200', '1600', '1310', '1320', '1340', '1350',
                           '1360', '1370', '1300', '1410', '1420', '1430', '1450', '1400', '1510', '1520', '1530',
                           '1540', '1550', '1500', '1700']
        financial_results_columns = ['2110', '2120', '2100', '2210', '2220', '2200', '2310', '2320', '2330', '2340',
                                     '2350', '2300', '2410', '2411', '2412', '2460', '2400', '2510', '2520', '2530',
                                     '2500', '2900', '2910', '2421', '2430', '2450']
        additional_financial_results_columns = ['2410', '2421', '2430', '2450']
        target_usage_columns = ['6100', '6210', '6215', '6220', '6230', '6240', '6250', '6200', '6310', '6311', '6312',
                                '6313', '6320', '6321', '6322', '6323', '6324', '6325', '6326', '6330', '6350', '6300',
                                '6400']
        capital_movement_columns = ['3100', '3210', '3211', '3212', '3213', '3214', '3215', '3216', '3220', '3221',
                                    '3222', '3223', '3224', '3225', '3226', '3227', '3230', '3240', '3200', '3310',
                                    '3311', '3312', '3313', '3314', '3315', '3316', '3320', '3321', '3322', '3323',
                                    '3324', '3325', '3326', '3327', '3330', '3340', '3300']
        capital_correctness_columns = ['3400', '3410', '3420', '3500', '3401', '3411', '3421', '3501', '3402', '3412',
                                       '3422', '3502']
        clean_assets_columns = ['3600']
        cash_flow_columns = ['4110', '4111', '4112', '4113', '4119', '4120', '4121', '4122', '4123', '4124', '4129',
                             '4100', '4210', '4211', '4212', '4213', '4214', '4219', '4220', '4221', '4222', '4223',
                             '4224', '4229', '4200', '4310', '4311', '4312', '4313', '4314', '4319', '4320', '4321',
                             '4322', '4323', '4329', '4300', '4400', '4450', '4500', '4490']

        balance_matrix = np.array(np.meshgrid(
            balance_columns,
            ['4', '5', '6'])).T.reshape(-1, 2)
        financial_results_matrix = np.array(np.meshgrid(
            financial_results_columns,
            ['4', '5'])).T.reshape(-1, 2)
        additional_financial_results_matrix = np.array(np.meshgrid(
            additional_financial_results_columns,
            ['4', '5'])).T.reshape(-1, 2)
        target_usage_matrix = np.array(np.meshgrid(
            target_usage_columns,
            ['4', '5'])).T.reshape(-1, 2)
        capital_movement_matrix = np.array(np.meshgrid(
            capital_movement_columns,
            ['8'])).T.reshape(-1, 2)
        capital_correctness_matrix = np.array(np.meshgrid(
            capital_correctness_columns,
            ['3', '6'])).T.reshape(-1, 2)
        cash_flow_matrix = np.array(np.meshgrid(
            cash_flow_columns,
            ['3', '4'])).T.reshape(-1, 2)

        def _parse_sheet(df: pd.DataFrame, matrix) -> pd.DataFrame:
            try:
                res = defaultdict(list)
                for code, column in matrix:
                    try:
                        find_code = df[df == code]
                        find_code_row = find_code.any(axis=1)
                        row = find_code_row.index[find_code_row][0]
                        find_code_column = find_code.any(axis=0)
                        tmp_column = find_code_column.index[find_code_column][0]
                        col = df.loc[:, tmp_column:][df.loc[:, tmp_column:] == column].any(axis=0)
                        col = col.index[col][0]
                        res[code].append(df.loc[row, col])
                    except:
                        res[code].append(None)
                return pd.DataFrame(res)
            except Exception as e:
                logger.exception(e)
                return pd.DataFrame()

        table_name: str = 'api_girbo_fundamentals'
        self.engine.execute(
            sa_text(f'''TRUNCATE TABLE {table_name}''').execution_options(autocommit=True))

        os.chdir(GirboData._download_dir)
        for filename in os.listdir('../../../api_model_python'):
            if filename.endswith('.zip'):
                filename_list = filename.split('_')
                inn = filename_list[-3]
                year = int(filename_list[-2])
                try:
                    with zipfile.ZipFile(filename, 'r') as zip_ref:
                        with zip_ref.open(zip_ref.namelist()[0]) as file_stream:
                            try:
                                balance_df = pd.read_excel(file_stream, sheet_name='Бухгалтерский баланс')
                            except:
                                logger.warning(f'No sheet "Бухгалтерский баланс" for inn {inn}')
                                balance_df = pd.DataFrame()
                            try:
                                finance_df = pd.read_excel(file_stream, sheet_name='Отчет о финансовых результатах')
                            except:
                                logger.warning(f'No sheet "Отчет о финансовых результатах" for inn {inn}')
                                finance_df = pd.DataFrame()
                            try:
                                target_usage_df = pd.read_excel(file_stream, sheet_name='Отчет о целевом использовании д')
                            except:
                                logger.warning(f'No sheet "Отчет о целевом использовании д" for inn {inn}')
                                target_usage_df = pd.DataFrame()
                            try:
                                capital_df = pd.read_excel(file_stream, sheet_name='Отчет об изменениях капитала')
                            except:
                                logger.warning(f'No sheet "Отчет об изменениях капитала" for inn {inn}')
                                capital_df = pd.DataFrame()
                            try:
                                cash_flow_df = pd.read_excel(file_stream, sheet_name='Отчет о движении денежных средс')
                            except:
                                logger.warning(f'No sheet "Отчет о движении денежных средс" for inn {inn}')
                                cash_flow_df = pd.DataFrame()
                    balance = _parse_sheet(balance_df, balance_matrix)

                    codes = finance_df[finance_df.isin(['Код строки', 'Код строк'])].stack().index.tolist()
                    rows_split = [row[0] for row in codes]
                    financial_results_df = pd.DataFrame()
                    additional_financial_results = pd.DataFrame()
                    if len(rows_split) == 1:
                        financial_results_df = finance_df.copy()
                    elif len(rows_split) == 2:
                        financial_results_df = finance_df[
                            (finance_df.index >= rows_split[0]) & (finance_df.index < rows_split[1])]
                        additional_financial_results_df = finance_df[(finance_df.index >= rows_split[1])]
                        additional_financial_results = _parse_sheet(additional_financial_results_df,
                                                                    additional_financial_results_matrix)
                    financial_results = _parse_sheet(financial_results_df, financial_results_matrix)

                    target_usage = _parse_sheet(target_usage_df, target_usage_matrix)
                    capital_movement = pd.DataFrame()
                    capital_correctness = pd.DataFrame()
                    clean_assets = pd.DataFrame()
                    if not capital_df.empty:
                        codes = capital_df[capital_df.isin(['Код строки', 'Код строк'])].stack().index.tolist()
                        rows_split = [row[0] for row in codes]
                        capital_movement_df = capital_df[
                            (capital_df.index >= rows_split[0]) & (capital_df.index < rows_split[1])]
                        capital_correctness_df = capital_df[
                            (capital_df.index >= rows_split[1]) & (capital_df.index < rows_split[2])]
                        clean_assets_df = capital_df[(capital_df.index >= rows_split[2])]

                        capital_movement = _parse_sheet(capital_movement_df, capital_movement_matrix)

                        capital_correctness = _parse_sheet(capital_correctness_df, capital_correctness_matrix)
                        capital_correctness = capital_correctness.iloc[::-1]

                        row = clean_assets_df[clean_assets_df == '3600'].any(axis=1)
                        row = row.index[row][0]
                        clean_assets = clean_assets_df.loc[row, :]
                        clean_assets = pd.DataFrame(clean_assets[clean_assets.notna()]).reset_index(drop=True).loc[
                                       2:].reset_index(drop=True)
                        clean_assets.columns = ['3600']
                    cash_flow = _parse_sheet(cash_flow_df, cash_flow_matrix)

                    balance.index = list(map(lambda x: year - x, balance.index))
                    financial_results.index = list(map(lambda x: year - x, financial_results.index))
                    additional_financial_results.index = list(
                        map(lambda x: year - x, additional_financial_results.index))
                    target_usage.index = list(map(lambda x: year - x, target_usage.index))
                    capital_movement.index = list(map(lambda x: year - x, capital_movement.index))
                    capital_correctness.index = list(map(lambda x: year - 1 - x, capital_correctness.index))
                    clean_assets.index = list(map(lambda x: year - x, clean_assets.index))
                    cash_flow.index = list(map(lambda x: year - x, cash_flow.index))

                    df: pd.DataFrame = pd.concat(
                        [balance, financial_results, additional_financial_results, target_usage,
                         capital_movement, capital_correctness, clean_assets, cash_flow],
                        axis=1)
                    for col in (balance_columns + financial_results_columns + additional_financial_results_columns + target_usage_columns +
                                capital_movement_columns + capital_correctness_columns + clean_assets_columns + cash_flow_columns):
                        if col not in df.columns:
                            df[col] = None
                    df = df.groupby(level=0, axis=1).first()
                    for col in df.select_dtypes(include=['object', 'string']):
                        df[col] = (
                            df[col]
                            .str.replace('(-)2', '', regex=False)
                            .str.replace('(0)2', '', regex=False)
                            .str.replace(r'[()\sХ-]', '', regex=True))
                        df[col] = pd.to_numeric(
                            df[col],
                            downcast='float',
                            errors='ignore')
                    df.reset_index(names='year', inplace=True)
                    df['inn'] = inn
                    df['reporting_year'] = year
                    df.to_sql(
                        name=table_name,
                        con=self.engine,
                        if_exists='append',
                        index=False)
                    logger.info(f'{inn} success')
                except Exception as e:
                    logger.exception(f'inn {inn} failed\n{e}')
