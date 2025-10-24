from datetime import date, timedelta
import pandas as pd
import xml.etree.ElementTree as ET
from src.sources.CBR.CBR import CBR


class KeyRate(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=KeyRate
    """

    def __init__(
        self,
        from_date: str = (date.today() - timedelta(days=30)).strftime('%Y-%m-%dT23:59:59'),
        to_date: str = date.today().strftime('%Y-%m-%dT23:59:59')
    ):
        super().__init__()
        self.params: dict[str, str] = {
            'fromDate': from_date,
            'ToDate': to_date
        }

    def parse_response(self) -> pd.DataFrame:
        # Parse XML
        if self.root is None:
            self.get_element()
        entries = self.root.findall(".//KR", CBR.namespaces)

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
        self.df = df
        return self.df
