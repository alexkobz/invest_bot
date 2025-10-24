from datetime import date

import pandas as pd
import xml.etree.ElementTree as ET
from src.sources.CBR.CBR import CBR


class GetCursOnDate(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=GetCursOnDate
    """

    def __init__(
        self,
        on_date: str = date.today().strftime('%Y-%m-%d')
    ):
        super().__init__()
        self.params: dict[str, str] = {
            'On_date': on_date
        }

    def _parse_response(self, root: ET.Element) -> pd.DataFrame:
        # Parse XML
        entries = root.findall('.//diffgr:diffgram//ValuteCursOnDate', CBR.namespaces)

        # Extract values
        currencies = []
        for val in entries:
            currency = {
                'name': val.find('Vname').text.strip() if val.find('Vname') is not None else None,
                'nom': float(val.find('Vnom').text) if val.find('Vnom') is not None else None,
                'curs': float(val.find('Vcurs').text) if val.find('Vcurs') is not None else None,
                'code': val.find('Vcode').text if val.find('Vcode') is not None else None,
                'chCode': val.find('VchCode').text if val.find('VchCode') is not None else None,
                'unitRate': float(val.find('VunitRate').text) if val.find('VunitRate') is not None else None,
                'date': self.params['On_date']
            }
            currencies.append(currency)

        # В виде pandas DataFrame
        df = pd.DataFrame(currencies)
        return df

