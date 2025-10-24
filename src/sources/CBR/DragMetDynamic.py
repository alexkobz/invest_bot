from datetime import date, datetime, timedelta
import pandas as pd
from src.sources.CBR.CBR import CBR


class DragMetDynamic(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=DragMetDynamic
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
        # Metal code mapping (based on common precious metals)
        metal_codes = {
            '1': 'Gold',
            '2': 'Silver',
            '3': 'Platinum',
            '4': 'Palladium'
        }
        if self.root is None:
            self.get_element()
        # Find all DrgMet elements
        drg_met_elements = self.root.findall(
            './/{urn:schemas-microsoft-com:xml-diffgram-v1}diffgram/DragMetall/DrgMet'
        )

        data = []
        for element in drg_met_elements:
            date_str = element.find('DateMet').text
            code = element.find('CodMet').text
            price = element.find('price').text

            # Parse date and format it
            date_obj = datetime.fromisoformat(date_str)
            formatted_date = date_obj.strftime('%Y-%m-%d')

            record = {
                'date': formatted_date,
                'code': code,
                'metal_name': metal_codes.get(code, f'Unknown_{code}'),
                'price': float(price) if price else None
            }
            data.append(record)
        self.df = pd.DataFrame(data)
        return self.df