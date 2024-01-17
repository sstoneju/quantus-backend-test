import traceback
import argparse
from loguru import logger
from datetime import datetime as dt
import pandas as pd

from service.collector import DartCollector, KrxCollector
from service.transform import Transform
from storage.csv import CsvStorage


def main(func, year):
    try:
        if func == "krx_market_cap_by_ticker":
            # NOTE python main.py --func krx_market_cap_by_ticker
            krx_api = KrxCollector()
            result = krx_api.get_market_cap_by_ticker(from_date='20230101', to_date='20240114', market="KOSPI")
            result.to_csv("market_cap_by_ticker_kospi.csv")
        if func == "sorted_result_under_50":
            # NOTE python main.py --func sorted_result_under_50
            krx_api = KrxCollector()
            result = krx_api.get_market_cap_by_ticker(from_date='20240115', to_date='20240115', market="KOSPI")
            sorted_result = result.sort_values(by='시가총액')
            extract_result = sorted_result.head(int(len(sorted_result) * 0.5))
            extract_result.to_csv("sorted_result_under_50.csv")
        if func == "krx_market_ohlcv_by_ticker":
            # NOTE python main.py --func krx_market_ohlcv_by_ticker
            krx_api = KrxCollector()
            result = krx_api.get_market_ohlcv_by_ticker(from_date='20230101', to_date='20240114', market="KOSPI")
            result.to_csv("market_ohlcv_kospi.csv")
        if func == "fix_market_cap_by_ticker":
            # NOTE python main.py --func fix_market_cap_by_ticker
            pd_data = pd.read_csv('market_cap_by_ticker.csv', index_col=0 )
            pd_data = pd_data[pd_data['종가'] != 0]
            pd_data.to_csv("market_cap_by_ticker_수정.csv")
        if func == "dart_fs_by_corp":
            # NOTE python main.py --func dart_fs_by_corp
            dart_api = DartCollector()
            result = dart_api.dart_fs_by_corp(from_date='20220101', to_date='20240114')
            logger.info(result)
        if func == "dart_fs_count":
            # NOTE python main.py --func dart_fs_count
            sorted_list = pd.read_csv('현금흐름표_20220101_20240114.csv')['stock_code'].drop_duplicates().tolist()
            logger.info(f"{len(sorted_list)}:  {sorted_list}")
        if func == "dart_corp_info":
            # NOTE python main.py --func dart_corp_info
            dart_api = DartCollector()
            result = dart_api.dart_fs_by_day(from_date='20230101', to_date='20240114')
            logger.info(result)

        # NOTE 데이터 transform
        if func == "trans_fs_bs":
            # NOTE python main.py --func trans_fs_bs
            stock_ohlcv = 'market_ohlcv_kospi.csv'
            market_cap = 'market_cap_by_ticker_kospi.csv'
            transform = Transform(stock_ohlcv, market_cap)
            transform.ffill_fs_bs('연결재무상태표', '20230101', '20240101')

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