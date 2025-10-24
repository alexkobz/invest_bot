import pandas as pd
from src.sources.CBR.CBR import CBR


class MainInfoXML(CBR):

    def parse_response(self) -> pd.DataFrame:

        if self.root is None:
            self.get_element()
        reg_data = self.root.find(".//RegData")
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
        self.df = pd.DataFrame(indicators)
        return self.df
