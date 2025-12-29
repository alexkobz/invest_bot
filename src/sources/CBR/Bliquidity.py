from datetime import date, datetime, timedelta

import pandas as pd
from zeep.helpers import serialize_object

from src.sources.CBR.CBR import CBR, CBRStageSaver


class Bliquidity(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=Bliquidity
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
        response = self.service.Bliquidity(
            fromDate=self.from_date,
            ToDate=self.to_date,
        )
        # Convert Zeep object → plain Python types
        data = serialize_object(response)
        records = []
        for item in data['_value_1']['_value_1']:
            rec = item['BL']
            records.append({
                'DT': rec['DT'],
                'StrLiDefNew': float(rec['StrLiDefNew']),
                'StrLiDef': float(rec['StrLiDef']),
                'claims': float(rec['claims']),
                'actionBasedRepoFX': float(rec['actionBasedRepoFX']),
                'actionBasedSecureLoans': float(rec['actionBasedSecureLoans']),
                'standingFacilitiesRepoFX': float(rec['standingFacilitiesRepoFX']),
                'standingFacilitiesSecureLoans': float(rec['standingFacilitiesSecureLoans']),
                'liabilities': float(rec['liabilities']),
                'depositAuctionBased': float(rec['depositAuctionBased']),
                'depositStandingFacilities': float(rec['depositStandingFacilities']),
                'CBRbonds': float(rec['CBRbonds']),
                'netCBRclaims': float(rec['netCBRclaims']),
                'CorrAcc': float(rec['CorrAcc']),
                'AVGRR': float(rec['AVGRR']),
            })

        df = pd.DataFrame(records)
        df['DT'] = pd.to_datetime(df['DT'].apply(lambda x: x.replace(tzinfo=None)).dt.date)
        self.df = df
        return self.df

    @CBRStageSaver(table_name='Bliquidity')
    def run(self):
        return super().run()
