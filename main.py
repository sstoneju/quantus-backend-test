import traceback
import argparse
from loguru import logger
from datetime import datetime as dt
import pandas as pd

from service.collector import DartCollector, KrxCollector
from storage.csv import CsvStorage


def main(func, year):
    try:
        if func == "krx_market_cap_by_ticker":
            # NOTE python main.py --func krx_market_cap_by_ticker
            krx_api = KrxCollector()
            result = krx_api.get_market_cap_by_ticker(from_date='20230101', to_date='20240114', market="ALL")
            result.to_csv("market_cap_by_ticker.csv")
        if func == "krx_market_ohlcv_by_ticker":
            # NOTE python main.py --func krx_market_ohlcv_by_ticker
            krx_api = KrxCollector()
            result = krx_api.get_market_ohlcv_by_ticker(from_date='20230101', to_date='20240114', market="ALL")
            result.to_csv("market_ohlcv.csv")
        if func == "fix_market_cap_by_ticker":
            # NOTE python main.py --func fix_market_cap_by_ticker
            pd_data = pd.read_csv('market_cap_by_ticker.csv', index_col=0 )
            pd_data = pd_data[pd_data['종가'] != 0]
            pd_data.to_csv("market_cap_by_ticker_수정.csv")
        if func == "dart_corp_info":
            # NOTE python main.py --func dart_corp_info
            dart_api = DartCollector()
            result = dart_api.check_all_company()
            logger.info(result)

        # NOTE 데이터 transform

        # NOTE 데이터 백테스트

    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--func', type=str)
    parser.add_argument('--year', type=int)

    args = parser.parse_args()
    func = args.func if "func" in args else ""
    year = args.year if "year" in args else ""

    main(func, year)