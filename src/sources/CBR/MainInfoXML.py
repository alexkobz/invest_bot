from datetime import date

import pandas as pd

from src.sources.CBR.CBR import CBR, CBRStageSaver


class MainInfoXML(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=MainInfoXML
    """

    def parse_response(self) -> pd.DataFrame:
        if self.service is None:
            self.get_service()
        response = self.service.MainInfoXML()
        data = {child.tag: float(child.text) for child in response}
        df = pd.DataFrame([data])
        df['date'] = pd.to_datetime(date.today())
        self.df = df
        return self.df

    @CBRStageSaver(table_name='MainInfoXML')
    def run(self):
        return super().run()
