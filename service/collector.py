from api.dart_api import DartAPI
from api.krx_api import KrxAPI

class DartCollector():
    api: DartAPI

    def __init__(self, api: DartAPI):
        self.api = api

    def collect_fs(self, years = [], report_codes=['11011', '11013', '11012', '11014']):
        """
        모든 회사, 연도, 보고서 코드에 대해 데이터 수집.
        """
        collected_data = []

        companies = self.api.fetch_corp_code()

        for company in companies:
            for year in years:
                for report_code in report_codes:
                        # TODO numpy로 변경하기
                        collected_data.extend(self.api.single_company_fs(company['corp_code'], year, report_code))

        return collected_data
    

class KrxCollector():
    api: KrxAPI

    def __init__(self, api:KrxAPI) -> None:
        self.api = api


    def all_stock(self):
        return self.api.market_ticker_and_name(self.api._today(), market="ALL")