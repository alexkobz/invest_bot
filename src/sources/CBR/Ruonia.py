from datetime import date, timedelta
import pandas as pd
from src.sources.CBR.CBR import CBR


class Ruonia(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=Ruonia
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
        # Find all ro elements
        ruonia_data = []
        if self.root is None:
            self.get_element()
        for ro in self.root.findall('.//diffgr:diffgram/Ruonia/ro', CBR.namespaces):
            data = {}

            # Extract date (D0)
            d0_element = ro.find('D0')
            if d0_element is not None and d0_element.text:
                data['date'] = d0_element.text

            # Extract rate (ruo)
            ruo_element = ro.find('ruo')
            if ruo_element is not None and ruo_element.text:
                data['rate'] = float(ruo_element.text)

            # Extract volume (vol)
            vol_element = ro.find('vol')
            if vol_element is not None and vol_element.text:
                data['volume'] = float(vol_element.text)

            # Extract update date (DateUpdate)
            date_update_element = ro.find('DateUpdate')
            if date_update_element is not None and date_update_element.text:
                data['date_update'] = date_update_element.text

            ruonia_data.append(data)

        self.df = pd.DataFrame(ruonia_data)
        return self.df