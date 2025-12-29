from datetime import date, datetime, timedelta

import pandas as pd
from zeep.helpers import serialize_object

from src.sources.CBR.CBR import CBR, CBRStageSaver


class Ruonia(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=Ruonia
    """

    def __init__(
        self,
        from_date: date = date.today() - timedelta(days=30),
        to_date: date = date.today(),
    ):
        super().__init__()
        if isinstance(from_date, str):
            from_date = datetime.strptime(from_date, "%Y-%m-%d")
        if isinstance(to_date, str):
            to_date = datetime.strptime(to_date, "%Y-%m-%d")
        self.from_date = from_date
        self.to_date = to_date


    def parse_response(self) -> pd.DataFrame:
        if self.service is None:
            self.get_service()
        response = self.service.Ruonia(
            fromDate=self.from_date,
            ToDate=self.to_date,
        )
        # Convert Zeep object → plain Python types
        data = serialize_object(response)
        items = data.get('_value_1', {}).get('_value_1', [])
        records = [
            {
                'date': rec['ro']['D0'],
                'rate': float(rec['ro']['ruo']),
                'volume': float(rec['ro']['vol']),
                'date_update': rec['ro']['DateUpdate']
            }
            for rec in items
        ]
        df = pd.DataFrame(records)
        df['date'] = pd.to_datetime(df['date'].apply(lambda x: x.replace(tzinfo=None)))
        df['date_update'] = pd.to_datetime(df['date_update'].apply(lambda x: x.replace(tzinfo=None)))
        self.df = df
        return self.df

    @CBRStageSaver(table_name='Ruonia')
    def run(self):
        return super().run()
