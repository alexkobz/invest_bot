from datetime import date, datetime, timedelta

import pandas as pd
from zeep.helpers import serialize_object

from src.sources.CBR.CBR import CBR, CBRStageSaver


class KeyRate(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=KeyRate
    """

    def __init__(
        self,
        from_date: date = date.today() - timedelta(days=365),
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
        response = self.service.KeyRate(
            fromDate=self.from_date,
            ToDate=self.to_date,
        )
        # Convert Zeep object → plain Python types
        data = serialize_object(response)
        # The structure is nested; KeyRateResult → _value_1 → _value_1 → list of rows
        records = [
            {'date': item['KR']['DT'], 'rate': item['KR']['Rate']}
            for item in data['_value_1']['_value_1']
        ]
        # Build a DataFrame
        df = pd.DataFrame(records)
        df['date'] = pd.to_datetime(df['date'].apply(lambda x: x.replace(tzinfo=None)).dt.date)
        df['rate'] = df['rate'].astype(float)
        self.df = df
        return self.df

    @CBRStageSaver(table_name='KeyRate')
    def run(self):
        return super().run()
