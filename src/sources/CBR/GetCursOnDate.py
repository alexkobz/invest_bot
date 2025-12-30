from datetime import date, datetime, timedelta

import pandas as pd
from zeep.helpers import serialize_object

from src.sources.CBR.CBR import CBR, CBRStageSaver


class GetCursOnDate(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=GetCursOnDate
    """

    def __init__(
        self,
        on_date: date = date.today() - timedelta(days=1),
    ):
        super().__init__()
        if isinstance(on_date, str):
            on_date = datetime.strptime(on_date, "%Y-%m-%d")
        self.on_date = on_date


    def parse_response(self) -> pd.DataFrame:
        if self.service is None:
            self.get_service()
        response = self.service.GetCursOnDate(self.on_date)
        # Convert Zeep object → plain Python types
        data = serialize_object(response)
        currencies = [
            v['ValuteCursOnDate']
            for v in data['_value_1']['_value_1']
        ]
        # Convert to DataFrame
        df = pd.DataFrame(currencies)
        # Clean names
        df.columns = ["name", "nom", "curs", "code", "chCode", "unitRate"]
        df['name'] = df['name'].str.strip()
        df['date'] = pd.to_datetime(self.on_date)
        self.df = df
        return self.df

    @CBRStageSaver(table_name='GetCursOnDate')
    def run(self):
        return super().run()
