from time import sleep
import pandas as pd
from pandas import DataFrame
from loguru import logger
from datetime import datetime as dt, timedelta
from pykrx.website.krx import market as krx_market


class KrxAPI():
    krx_url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    
    headers = {
        "Origin":"http://data.krx.co.kr",
        "Referer":"http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd"
    }

    mktId = ["All", "STK", "KSQ", "KNX"]

    def _today(self):
        return dt.today().strftime("%Y%m%d")
    
    def get_market_ohlcv_by_ticker(self, from_date='', to_date = "", market="ALL") -> DataFrame:
        """
        기간 범위에 있는 모든 종목의 OHLCV
        NOTE 날짜를 기준으로 데이터를 모으기 때문에 상폐 당한 데이터도 함께 포함해서 받을 수 있다.
        """
        from_date = from_date if from_date and isinstance(from_date, str) else self._today()
        to_date = to_date if to_date and isinstance(to_date, str) else self._today()
        
        market_ohlcv_list = DataFrame()
        while from_date <= to_date:
            trade_date = from_date
            logger.info(f"get_market_cap_by_ticker - trade_date: {trade_date}")
            from_date = dt.strptime(from_date, "%Y%m%d") + timedelta(days=1)
            from_date = from_date.strftime("%Y%m%d")
            result = krx_market.get_market_ohlcv_by_ticker(trade_date, market)
            result['티커'] = result.index
            result['trade_date'] = trade_date

            logger.info(f"from_date: {from_date}")
            logger.info(f"to_date: {to_date}")
            logger.info(f"trade_date: {trade_date}")
            logger.info(f"market: {market}")
            logger.info(f"result: \n{result.head()}")
            sleep(1)

            # NOTE KRX 요청 전 working date로 filtering 하는게 더 좋아보임.
            if result['거래량'].iloc[0] != 0:
                logger.info("concat data")
                market_ohlcv_list = pd.concat([market_ohlcv_list, result], ignore_index=True)

        return market_ohlcv_list

    def get_market_cap_by_ticker(self, from_date = "", to_date = "", market="ALL") -> DataFrame:
        """
        기간 범위에 있는 모든 종목의 시가총액
        NOTE 날짜를 기준으로 데이터를 모으기 때문에 상폐 당한 데이터도 함께 포함해서 받을 수 있다.
        ex) 069110, 코스온, 20231020 상폐일
        """
        from_date = from_date if from_date and isinstance(from_date, str) else self._today()
        to_date = to_date if to_date and isinstance(to_date, str) else self._today()

        market_cap_list = DataFrame()

        while from_date <= to_date:
            trade_date = from_date
            logger.info(f"get_market_cap_by_ticker - trade_date: {trade_date}")
            from_date = dt.strptime(from_date, "%Y%m%d") + timedelta(days=1)
            from_date = from_date.strftime("%Y%m%d")
            result = krx_market.get_market_cap_by_ticker(trade_date, market)
            result['티커'] = result.index
            result['trade_date'] = trade_date

            logger.info(f"from_date: {from_date}")
            logger.info(f"to_date: {to_date}")
            logger.info(f"trade_date: {trade_date}")
            logger.info(f"market: {market}")
            logger.info(f"result: \n{result.head()}")
            sleep(1)

            if result['종가'].iloc[0] != 0:
                logger.info("concat data")
                market_cap_list = pd.concat([market_cap_list, result], ignore_index=True)

        return market_cap_list

    def get_market_price_change_by_ticker(self, fromdate:str, todate:str):
        """
        입력된 기간동안의 전 종목 수익률 반환
        """
        fromdate = fromdate if fromdate and isinstance(fromdate, str) else self._today()
        todate = todate if todate and isinstance(todate, str) else self._today()

        return krx_market.get_market_price_change_by_ticker(fromdate, todate, market="ALL", adjusted=True)