import pandas as pd
import xml.etree.ElementTree as ET
from src.sources.CBR.CBR import CBR


class MainInfoXML(CBR):

    def _parse_response(self, root: ET.Element) -> pd.DataFrame:
        reg_data = root.find(".//RegData")
        if reg_data is None:
            print("No RegData found in response")
            return pd.DataFrame()

        indicators = []
        for element in reg_data:
            indicators.append({
                'indicator': element.tag,
                'title': element.get('Title'),
                'date': element.get('Date'),
                'value': float(element.text) if element.text else None
            })
        df = pd.DataFrame(indicators)
        return df
