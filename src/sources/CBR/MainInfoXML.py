import pandas as pd
from datetime import date
from src.sources.CBR.CBR import CBR


class MainInfoXML(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=MainInfoXML
    """

    def parse_response(self) -> pd.DataFrame:
        response = self.service.MainInfoXML()
        data = {child.tag: float(child.text) for child in response}
        df = pd.DataFrame([data])
        df['Date'] = pd.to_datetime(date.today())
        self.df = df
        return self.df
