from datetime import date, datetime, timedelta

import pandas as pd
from zeep.helpers import serialize_object

from src.sources.CBR.CBR import CBR, CBRStageSaver


class DragMetDynamic(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=DragMetDynamic
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
        metal_codes = {
            1: 'gold',
            2: 'silver',
            3: 'platinum',
            4: 'palladium'
        }
        if self.service is None:
            self.get_service()
        response = self.service.DragMetDynamic(
            fromDate=self.from_date,
            ToDate=self.to_date,
        )
        data = serialize_object(response)
        items = data.get('_value_1', {}).get('_value_1', [])
        records = [
            {
                'date': rec['DrgMet']['DateMet'],
                'code': int(rec['DrgMet']['CodMet']),
                'metal_name': metal_codes.get(int(rec['DrgMet']['CodMet']), f'Unknown_{int(rec['DrgMet']['CodMet'])}'),
                'price': float(rec['DrgMet']['price'])
            }
            for rec in items
        ]
        df = pd.DataFrame(records)
        df['date'] = pd.to_datetime(df['date'].apply(lambda x: x.replace(tzinfo=None)).dt.date)
        self.df = df
        return self.df

    @CBRStageSaver(table_name='DragMetDynamic')
    def run(self):
        return super().run()
