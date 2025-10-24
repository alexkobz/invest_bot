from datetime import date, timedelta
import pandas as pd
from src.sources.CBR.CBR import CBR


class GetCursDynamic(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=GetCursDynamic
    """

    def __init__(
        self,
        from_date: str = date(1990, 1, 1).strftime('%Y-%m-%dT23:59:59'),
        to_date: str = date.today().strftime('%Y-%m-%dT23:59:59'),
        currency : str = 'R01235',
    ):
        super().__init__()
        self.params: dict[str, str] = {
            'fromDate': from_date,
            'ToDate': to_date,
            'ValutaCode': currency,
        }

    def parse_response(self) -> pd.DataFrame:
        # Parse XML
        if self.root is None:
            self.get_element()
        valute_data = self.root.find(f'.//http://web.cbr.ru/ValuteData')
        if valute_data is None:
            valute_data = self.root.find('.//ValuteData')

        if valute_data is None:
            print("ValuteData element not found")
            return []

        currency_rates = []

        for valute in valute_data.findall('ValuteCursDynamic'):
            rate_data = {}

            # Extract each field
            curs_date = valute.find('CursDate')
            vcode = valute.find('Vcode')
            vnom = valute.find('Vnom')
            vcurs = valute.find('Vcurs')
            vunit_rate = valute.find('VunitRate')

            if curs_date is not None and curs_date.text:
                rate_data['date'] = curs_date.text

            if vcode is not None and vcode.text:
                rate_data['currency_code'] = vcode.text

            if vnom is not None and vnom.text:
                rate_data['nominal'] = int(vnom.text)

            if vcurs is not None and vcurs.text:
                rate_data['exchange_rate'] = float(vcurs.text)

            if vunit_rate is not None and vunit_rate.text:
                rate_data['unit_rate'] = float(vunit_rate.text)

            # Also get the diffgr attributes if needed
            rate_data['id'] = valute.get('{urn:schemas-microsoft-com:xml-diffgram-v1}id')
            rate_data['row_order'] = valute.get('{urn:schemas-microsoft-com:xml-msdata}rowOrder')

            currency_rates.append(rate_data)

        self.df = pd.DataFrame(currency_rates)
        return self.df

