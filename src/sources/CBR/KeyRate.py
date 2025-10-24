from datetime import date

import pandas as pd
import xml.etree.ElementTree as ET
from src.sources.CBR.CBR import CBR


class KeyRate(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=KeyRate
    """

    def __init__(
        self,
        from_date: str = '2013-09-16T00:00:00',
        to_date: str = date.today().strftime('%Y-%m-%dT23:59:59')
    ):
        super().__init__()
        self.params: dict[str, str] = {
            'fromDate': from_date,
            'ToDate': to_date
        }

    def _parse_response(self, root: ET.Element) -> pd.DataFrame:
        # Parse XML
        entries = root.findall(".//KR", CBR.namespaces)

        # Extract values
        result = []
        for kr in entries:
            DT = kr.find("DT").text
            rate = float(kr.find("Rate").text)
            result.append({"date": DT, "rate": rate})

        df = pd.DataFrame(result)
        # Data cleaning and type conversion
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'], utc=True).dt.date
            df['rate'] = pd.to_numeric(df['rate'], errors='coerce')

        return df
