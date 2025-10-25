from datetime import date, timedelta
import pandas as pd
from src.sources.CBR.CBR import CBR


class Bliquidity(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=Bliquidity
    """

    def __init__(
        self,
        from_date: str = date(1990, 1, 1).strftime('%Y-%m-%dT23:59:59'),
        to_date: str = date.today().strftime('%Y-%m-%dT23:59:59')
    ):
        super().__init__()
        self.params: dict[str, str] = {
            'fromDate': from_date,
            'ToDate': to_date
        }

    def parse_response(self) -> pd.DataFrame:
        if self.root is None:
            self.get_element()
        data = []
        for bl in self.root.findall('.//BL'):
            row = {}
            for child in bl:
                row[child.tag] = child.text
            data.append(row)

        self.df = pd.DataFrame(data)
        return self.df
