from datetime import date, datetime

import pandas as pd
import xml.etree.ElementTree as ET
from src.sources.CBR.CBR import CBR


class DragMetDynamic(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=DragMetDynamic
    """

    def __init__(
        self,
        from_date: str = '1997-03-25T00:00:00',
        to_date: str = date.today().strftime('%Y-%m-%dT23:59:59')
    ):
        super().__init__()
        self.params: dict[str, str] = {
            'fromDate': from_date,
            'ToDate': to_date
        }

    def _parse_response(self, root: ET.Element) -> pd.DataFrame:
        # Metal code mapping (based on common precious metals)
        metal_codes = {
            '1': 'Gold',
            '2': 'Silver',
            '3': 'Platinum',
            '4': 'Palladium'
        }

        # Find all DrgMet elements
        drg_met_elements = root.findall(
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
        df = pd.DataFrame(data)
        return df