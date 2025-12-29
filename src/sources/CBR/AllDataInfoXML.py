import pandas as pd

from src.sources.CBR.CBR import CBR


class AllDataInfoXML(CBR):

    def currency(self) -> pd.DataFrame:
        currency_data = []
        currency = self.root.find('.//Currency', CBR.namespaces)
        if currency is not None:
            for curr in currency:
                if curr.tag in ['USD', 'EUR', 'CNY']:
                    date = curr.get('OnDate', 'N/A')
                    rate = curr.find('curs').text if curr.find('curs') is not None else 'N/A'

                    currency_data.append({
                        'currency': curr.tag,
                        'rate': float(rate) if rate != 'N/A' else None,
                        'date': date,
                        'last_update': currency.get('LUpd')
                    })

        df_currency = pd.DataFrame(currency_data).reset_index()
        return df_currency


    def metals(self) -> pd.DataFrame:
        metals_data = []
        metall = self.root.find('.//Metall', CBR.namespaces)
        if metall is not None:
            print(f"Last Update: {metall.get('LUpd')}")
            for metal in metall:
                name = metal.tag
                current_val = metal.get('val', 'N/A')
                old_val = metal.get('old_val', 'N/A')

                metals_data.append({
                    'metal': name,
                    'current_price': float(current_val) if current_val != 'N/A' else None,
                    'previous_price': float(old_val) if old_val != 'N/A' else None,
                    'date': metall.get('OnDate'),
                    'last_update': metall.get('LUpd')
                })

        df_metals = pd.DataFrame(metals_data).reset_index()
        return df_metals

    def key_rate(self) -> pd.DataFrame:
        key_rates_data = []

        key_rate = self.root.find('.//KEY_RATE', CBR.namespaces)
        if key_rate is not None:
            rate_val = key_rate.get('val')
            rate_date = key_rate.get('date')

            key_rates_data.append({
                'type': 'current',
                'rate': float(rate_val) if rate_val else None,
                'date': rate_date,
                'title': key_rate.get('Title', 'Key Rate')
            })

        key_rate_future = self.root.find('.//KEY_RATE_FUTURE', CBR.namespaces)
        if key_rate_future is not None:
            future_rate = key_rate_future.get('val')
            future_date = key_rate_future.get('newdate')

            key_rates_data.append({
                'type': 'future',
                'rate': float(future_rate) if future_rate else None,
                'date': future_date,
                'title': key_rate_future.get('Title', 'Future Key Rate')
            })

        df_key_rates = pd.DataFrame(key_rates_data).reset_index()
        return df_key_rates

    def inflation(self) -> pd.DataFrame:
        inflation_data = []

        inflation = self.root.find('.//Inflation', CBR.namespaces)
        if inflation is not None:
            inflation_rate = inflation.get('val')
            inflation_date = inflation.get('OnDate')

            inflation_data.append({
                'indicator': 'inflation',
                'rate': float(inflation_rate) if inflation_rate else None,
                'date': inflation_date,
                'title': inflation.get('Title', 'Inflation')
            })

        inflation_target = self.root.find('.//InflationTarget', CBR.namespaces)
        if inflation_target is not None:
            target_rate = inflation_target.get('val')
            target_date = inflation_target.get('OnDate')

            inflation_data.append({
                'indicator': 'target',
                'rate': float(target_rate) if target_rate else None,
                'date': target_date,
                'title': inflation_target.get('Title', 'Inflation Target')
            })

        df_inflation = pd.DataFrame(inflation_data).reset_index()
        return df_inflation

    def ruonia(self) -> pd.DataFrame:
        ruonia_data = []

        ruonia = self.root.find('.//RUONIA', CBR.namespaces)
        if ruonia is not None:
            d1 = ruonia.find('.//D1', CBR.namespaces)
            if d1 is not None:
                current_val = d1.get('val')
                old_val = d1.get('old_val', 'N/A')

                ruonia_data.append({
                    'period': '1_day',
                    'current_rate': float(current_val) if current_val else None,
                    'previous_rate': float(old_val) if old_val != 'N/A' else None,
                    'date': ruonia.get('OnDate'),
                    'last_update': ruonia.get('LUpd'),
                    'title': ruonia.get('Title', 'RUONIA')
                })

        # SV RUONIA (Term RUONIA)
        sv_ruonia = self.root.find('.//SV_RUONIA', CBR.namespaces)
        if sv_ruonia is not None:
            for period in sv_ruonia:
                period_name = period.tag  # M1, M3, M6
                current_val = period.get('val')
                old_val = period.get('old_val', 'N/A')

                if period_name in ['M1', 'M3', 'M6']:
                    print(f"RUONIA {period_name}: {current_val}% (Previous: {old_val}%)")

                    ruonia_data.append({
                        'period': period_name.lower(),
                        'current_rate': float(current_val) if current_val else None,
                        'previous_rate': float(old_val) if old_val != 'N/A' else None,
                        'date': sv_ruonia.get('OnDate'),
                        'last_update': sv_ruonia.get('LUpd'),
                        'title': sv_ruonia.get('Title', 'Term RUONIA')
                    })

        df_ruonia = pd.DataFrame(ruonia_data).reset_index()
        return df_ruonia

    def banking_liquidity(self) -> pd.DataFrame:
        liquidity_data = []

        bank_likvid = self.root.find('.//BankLikvid', CBR.namespaces)
        if bank_likvid is not None:
            for indicator in bank_likvid:
                title = indicator.get('Title', indicator.tag)

                # Handle different structures
                if indicator.get('val'):  # Direct value
                    current_val = indicator.get('val')
                    old_val = indicator.get('old_val', 'N/A')
                    date_val = indicator.get('OnDate', 'N/A')
                else:  # Nested structure
                    value_elem = indicator.find('.//*[@val]')
                    if value_elem is not None:
                        current_val = value_elem.get('val')
                        old_val = value_elem.get('old_val', 'N/A')
                        date_val = value_elem.get('OnDate', indicator.get('OnDate', 'N/A'))
                    else:
                        continue

                if current_val:
                    print(f"{title}: {current_val} (Previous: {old_val})")

                    liquidity_data.append({
                        'indicator': title,
                        'current_value': float(current_val) if current_val.replace('.', '').isdigit() else current_val,
                        'previous_value': float(old_val) if old_val != 'N/A' and old_val.replace('.',
                                                                                                 '').isdigit() else old_val,
                        'date': date_val,
                        'last_update': indicator.get('LUpd', 'N/A')
                    })

        df_liquidity = pd.DataFrame(liquidity_data).reset_index()
        return df_liquidity

    def parse_response(self) -> dict[str, pd.DataFrame]:
        if self.root is None:
            self.get_element()
        currency = self.currency()
        metals = self.metals()
        key_rate = self.key_rate()
        inflation = self.inflation()
        ruonia = self.ruonia()
        banking_liquidity = self.banking_liquidity()
        return {
            'currency': currency,
            'metals': metals,
            'key_rate': key_rate,
            'inflation': inflation,
            'ruonia': ruonia,
            'banking_liquidity': banking_liquidity,
        }

    def run(self) -> dict[str, pd.DataFrame]:
        self.send_request()
        self.get_element()
        dataframes: dict[str, pd.DataFrame] = self.parse_response()
        for tablename, dataframe in dataframes.items():
            self.method = tablename
            self.df = dataframe
        return dataframes

