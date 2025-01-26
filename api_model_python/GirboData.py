from __future__ import annotations

import os
from io import BytesIO

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
from typing import List, Dict, Union

from api_model_python.plugins.path import get_project_root, Path

from logs.Logger import Logger


logger = Logger()


class GirboData:

    _download_dir: str = '/opt/airflow/data/girbo_fundamentals'

    def __init__(self):
        env_path: Path = Path.joinpath(get_project_root(), '.env')
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
                FROM public_marts.dim_emitents
                WHERE inn NOT IN (
                    SELECT inn FROM public_marts.dim_girbo_organizations_cards
                )
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
            url = f"https://bo.nalog.ru/search?query={inn}"
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
        for filename in os.listdir('.'):
            file_path = os.path.join('.', filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
        cards: List[str] = (
            pd.read_sql(
                """
                SELECT DISTINCT card
                FROM public_marts.dim_girbo_organizations_cards
                WHERE card IS NOT NULL AND card != '' AND is_loaded IS false
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
            self.engine.execute(
                sa_text(
                    f"""
                    UPDATE public_marts.dim_girbo_organizations_cards
                    SET is_loaded = true
                    WHERE card = '{card}'
                    """
                ).execution_options(autocommit=True))
            logger.info(f"{card} success")
            sleep(1)

    def parse_fundamentals(self):

        import zipfile
        from openpyxl import load_workbook
        from dataclasses import dataclass
        from openpyxl.workbook import Workbook
        from openpyxl.worksheet.worksheet import Worksheet

        @dataclass
        class FundamentalsData:
            sheet: Worksheet
            cells_range: str
            rows: List[int]

        class FundamentalsParser:

            def __init__(self, wb):
                self.workbook: Workbook = wb
                self.main = FundamentalsData(
                    sheet=self._get_worksheet("Сведения об организации"),
                    cells_range="A10:M10",
                    rows=[0, 12])
                self.non_current_assets = FundamentalsData(
                    sheet=self._get_worksheet("Бухгалтерский баланс"),
                    cells_range="N9:AG19",
                    rows=[0, 4, 11, 19])
                self.current_assets = FundamentalsData(
                    sheet=self._get_worksheet("Бухгалтерский баланс"),
                    cells_range="N20:AG29",
                    rows=[0, 4, 11, 19])
                self.capital_reserves = FundamentalsData(
                    sheet=self._get_worksheet("Бухгалтерский баланс"),
                    cells_range="N30:AG37",
                    rows=[0, 4, 11, 19])
                self.longterm_liabilities = FundamentalsData(
                    sheet=self._get_worksheet("Бухгалтерский баланс"),
                    cells_range="N38:AG43",
                    rows=[0, 4, 11, 19])
                self.shortterm_liabilities = FundamentalsData(
                    sheet=self._get_worksheet("Бухгалтерский баланс"),
                    cells_range="N44:AG50",
                    rows=[0, 4, 11, 19])
                self.financial_results = FundamentalsData(
                    sheet=self._get_worksheet("Отчет о финансовых результатах"),
                    cells_range="Q7:AD30",
                    rows=[0, 6, 13])
                self.financial_results_addition = FundamentalsData(
                    sheet=self._get_worksheet("Отчет о финансовых результатах"),
                    cells_range="Q38:AD42",
                    rows=[0, 6, 13])
                self.target_usage_cash = FundamentalsData(
                    sheet=self._get_worksheet("Отчет о целевом использовании д"),
                    cells_range="P7:AC31",
                    rows=[0, 6, 13])
                self.capital_movement = FundamentalsData(
                    sheet=self._get_worksheet("Отчет об изменениях капитала"),
                    cells_range="F8:AF47",
                    rows=[0, 26])
                self.capital_correctness = FundamentalsData(
                    sheet=self._get_worksheet("Отчет об изменениях капитала"),
                    cells_range="J52:AE67",
                    rows=[0, 21])
                self.clean_assets = FundamentalsData(
                    sheet=self._get_worksheet("Отчет об изменениях капитала"),
                    cells_range="I69:L70",
                    rows=[0, 3])
                self.cash_flow = FundamentalsData(
                    sheet=self._get_worksheet("Отчет о движении денежных средс"),
                    cells_range="Q8:AD50",
                    rows=[0, 6, 13])

            def _get_worksheet(self, sheet_name):
                """Safely get a worksheet by name from the workbook."""
                try:
                    return self.workbook[sheet_name]
                except KeyError:
                    return None

            @staticmethod
            def parse_sheet(fundamentals_data: FundamentalsData):
                values = [[cell.value for cell in row] for row in fundamentals_data.sheet[fundamentals_data.cells_range]]
                df = pd.DataFrame(values).T
                df = df.loc[fundamentals_data.rows]
                df.columns = df.loc[0]
                df = df.drop(df.index[0]).reset_index(drop=True)
                try:
                    del df[None]
                except KeyError:
                    pass
                for col in df.select_dtypes(include=['object', 'string']):
                    df[col] = (
                        df[col]
                        .str.replace('(-)2', '', regex=False)
                        .str.replace(r'[()\sХ-]', '', regex=True))
                    df[col] = pd.to_numeric(
                        df[col],
                        downcast='float',
                        errors='ignore')
                df.index = list(map(lambda x: year - x, df.index))
                return df

        table_name: str = 'api_girbo_fundamentals'
        self.engine.execute(
            sa_text(f'''TRUNCATE TABLE {table_name}''').execution_options(autocommit=True))

        os.chdir(GirboData._download_dir)
        for filename in os.listdir('.'):
            if filename.endswith('.zip'):
                filename_list = filename.split('_')
                inn = filename_list[-3]
                year = int(filename_list[-2])
                try:
                    with zipfile.ZipFile(filename, 'r') as zip_ref:
                        with zip_ref.open(zip_ref.namelist()[0]) as file_stream:
                            workbook: Workbook = load_workbook(filename=BytesIO(file_stream.read()))
                    fundamentals_parser = FundamentalsParser(workbook)
                    non_current_assets: pd.DataFrame = fundamentals_parser.parse_sheet(
                        fundamentals_parser.non_current_assets)
                    current_assets: pd.DataFrame = fundamentals_parser.parse_sheet(
                        fundamentals_parser.current_assets)
                    capital_reserves: pd.DataFrame = fundamentals_parser.parse_sheet(
                        fundamentals_parser.capital_reserves)
                    longterm_liabilities: pd.DataFrame = fundamentals_parser.parse_sheet(
                        fundamentals_parser.longterm_liabilities)
                    shortterm_liabilities: pd.DataFrame = fundamentals_parser.parse_sheet(
                        fundamentals_parser.shortterm_liabilities)
                    financial_results: pd.DataFrame = fundamentals_parser.parse_sheet(
                        fundamentals_parser.financial_results)
                    financial_results_addition: pd.DataFrame = fundamentals_parser.parse_sheet(
                        fundamentals_parser.financial_results_addition)
                    target_usage_cash: pd.DataFrame = fundamentals_parser.parse_sheet(
                        fundamentals_parser.target_usage_cash)
                    capital_movement: pd.DataFrame = fundamentals_parser.parse_sheet(
                        fundamentals_parser.capital_movement)
                    capital_correctness: pd.DataFrame = fundamentals_parser.parse_sheet(
                        fundamentals_parser.capital_correctness)
                    clean_assets: pd.DataFrame = fundamentals_parser.parse_sheet(
                        fundamentals_parser.clean_assets)
                    cash_flow: pd.DataFrame = fundamentals_parser.parse_sheet(
                        fundamentals_parser.cash_flow)
                    inn_chunk: pd.DataFrame = pd.concat(
                        [non_current_assets, current_assets, capital_reserves, longterm_liabilities, shortterm_liabilities,
                         financial_results, financial_results_addition, target_usage_cash,
                         capital_movement, capital_correctness, clean_assets, cash_flow],
                        axis=1)
                    inn_chunk.reset_index(names='year', inplace=True)
                    inn_chunk['inn'] = inn
                    inn_chunk['reporting_year'] = year
                    inn_chunk.to_sql(
                        name=table_name,
                        con=self.engine,
                        if_exists='append',
                        index=False)
                    logger.info(f'{inn} success')
                except Exception as e:
                    logger.exception(e)
