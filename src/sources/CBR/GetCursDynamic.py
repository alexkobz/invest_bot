import pandas as pd
from datetime import date, datetime, timedelta
from zeep.helpers import serialize_object

from src.sources.CBR.CBR import CBR


class GetCursDynamic(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=GetCursDynamic
    """

    def __init__(
        self,
        from_date: date = date.today() - timedelta(days=30),
        to_date: date = date.today(),
        currency: str = 'USD',

    ):
        super().__init__()
        if isinstance(from_date, str):
            from_date = datetime.strptime(from_date, "%Y-%m-%d")
        if isinstance(to_date, str):
            to_date = datetime.strptime(to_date, "%Y-%m-%d")
        self.from_date = from_date
        self.to_date = to_date
        self.currency = currency



    def parse_response(self) -> pd.DataFrame:
        response = self.service.GetCursDynamic(
            fromDate=self.from_date,
            ToDate=self.to_date,
            ValutaCode=self.currency,
        )
        data = serialize_object(response)
        items = data['_value_1']['_value_1']
        records = [
            {
                'Date': item['ValuteCursOnDateDynamic']['CursDate'],
                'Code': item['ValuteCursOnDateDynamic']['VchCode'],
                'Nominal': float(item['ValuteCursOnDateDynamic']['Vnom']),
                'Rate': float(item['ValuteCursOnDateDynamic']['Vcurs']),
            }
            for item in items
        ]
        df = pd.DataFrame(records)
        df['Date'] = pd.to_datetime(df['Date'].apply(lambda x: x.replace(tzinfo=None)).dt.date)
        self.df = df
        return self.df
