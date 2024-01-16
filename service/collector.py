from calendar import month
import os
import re
from time import sleep
import pandas as pd
from pandas import DataFrame
from loguru import logger
from datetime import datetime as dt, timedelta

from pykrx.website.krx import market as krx_market

import dart_fss as Dart
from dart_fss.fs.extract import analyze_report
from dart_fss.filings.search_result import SearchResults
from dart_fss.filings.reports import Report
from dart_fss.errors.errors import NotFoundConsolidated, NoDataReceived, OverQueryLimit


class DartCollector():
    # NOTE 환경 변수로 빼놓기
    api_key = ["f25039bc5c5af07e0c9a788366ecfd58ca72674f",
               "d5ea58abd9def006340a95d00655011fb1a8255b",
               "3355e181b6492517bf43b4d3a2ce3c14ebd7c4bd"]
    key_sequence = 0
    dart: Dart

    def __init__(self, api_key = "") -> None:
        self.api_key = self.api_key.append(api_key) if api_key else self.api_key
        logger.info(self.api_key)
        logger.info(self.api_key[self.key_sequence])
        self.dart = Dart
        self.dart.set_api_key(self.api_key[self.key_sequence])
    
    def set_next_api_key(self):
        self.key_sequence += 1
        # TODO list 참조 예외 처리 필요
        self.dart.set_api_key(self.api_key[self.key_sequence])
    
    def _today(self):
        return dt.today().strftime("%Y%m%d")
    
    def _prepare_bs_fs(self, df: DataFrame):
        """
        재무상태표를 정리하는 함수
        """
        level_0 = df.columns.get_level_values(0).to_list()[7:]
        level_1 = df.columns.get_level_values(1).to_list()[:7]

        # 상위 인덱스 제거
        df = df.droplevel(level=0, axis=1)
        df.columns = level_1+level_0
        logger.info(df.columns)
        
        # 필요한 데이터 select
        columns_of_interest = ["유동자산", "비유동자산", "자산총계", "유동부채", "비유동부채", "부채총계", "자본총계", "부채와자본총계"]
        filterd_bs = df[df['label_ko'].isin(columns_of_interest)]

        # 공시일 별로 금액을 melt
        days_columns = filterd_bs.columns[7:]
        logger.info(days_columns)
        melted_df = pd.melt(filterd_bs, id_vars=filterd_bs.columns[:7], value_vars=days_columns, var_name="fs_date", value_name="amount")
        return melted_df
    
    def _prepare_is_fs(self, df: DataFrame):
        """
        손익계산서를 정리하는 함수
        """
        level_0 = df.columns.get_level_values(0).to_list()[5:]
        level_1 = df.columns.get_level_values(1).to_list()[:5]

        # 상위 인덱스 제거
        df = df.droplevel(level=0, axis=1)
        df.columns = level_1+level_0
        logger.info(df.columns)
        
        # 필요한 데이터 select
        columns_of_interest = ["수익(매출액)","매출원가","매출총이익","판매비와관리비","영업이익","당기순이익(손실)","기본주당이익(손실)"]
        filterd_bs = df[df['label_ko'].isin(columns_of_interest)]

        # 공시일 별로 금액을 melt
        days_columns = filterd_bs.columns[5:]
        logger.info(days_columns)
        melted_df = pd.melt(filterd_bs, id_vars=filterd_bs.columns[:5], value_vars=days_columns, var_name="fs_date", value_name="amount")
        
        return melted_df
    
    def _prepare_cis_fs(self, df: DataFrame):
        """
        포괄손익계산서를 정리하는 함수
        """
        level_0 = df.columns.get_level_values(0).to_list()[7:]
        level_1 = df.columns.get_level_values(1).to_list()[:7]

        # 상위 인덱스 제거
        df = df.droplevel(level=0, axis=1)
        df.columns = level_1+level_0
        logger.info(df.columns)
        
        # 필요한 데이터 select
        columns_of_interest = ["당기순이익(손실)", "총포괄손익"]
        filterd_bs = df[df['label_ko'].isin(columns_of_interest)]

        # 공시일 별로 금액을 melt
        days_columns = filterd_bs.columns[7:]
        logger.info(days_columns)
        melted_df = pd.melt(filterd_bs, id_vars=filterd_bs.columns[:7], value_vars=days_columns, var_name="fs_date", value_name="amount")
        return melted_df
    
    def _prepare_cf_fs(self, df: DataFrame):
        """
        현금흐름표를 정리하는 함수
        """
        level_0 = df.columns.get_level_values(0).to_list()[7:]
        level_1 = df.columns.get_level_values(1).to_list()[:7]

        # 상위 인덱스 제거
        df = df.droplevel(level=0, axis=1)
        df.columns = level_1+level_0
        logger.info(df.columns)
        
        # 필요한 데이터 select
        columns_of_interest = ["영업활동 현금흐름", "투자활동 현금흐름", "재무활동 현금흐름", "기초의 현금및현금성자산", "기말의 현금및현금성자산"]
        filterd_bs = df[df['label_ko'].isin(columns_of_interest)]

        # 공시일 별로 금액을 melt
        days_columns = filterd_bs.columns[7:]
        logger.info(days_columns)
        melted_df = pd.melt(filterd_bs, id_vars=filterd_bs.columns[:7], value_vars=days_columns, var_name="fs_date", value_name="amount")
        return melted_df

    def _get_fs(self, report: Report):
        try:
            logger.info(report)
            fs = analyze_report(report)
            fs_type = ['bs', 'is', 'cis', 'cf']
            for f in fs_type:
                if not fs[f].empty:
                    if f == 'bs':
                        prepared_df = self._prepare_bs_fs(fs[f])
                    if f == 'is':
                        prepared_df = self._prepare_is_fs(fs[f])
                    if f == 'cis':
                        prepared_df = self._prepare_cis_fs(fs[f])
                    if f == 'cf':
                        prepared_df = self._prepare_cf_fs(fs[f])

                    prepared_df['rcp_no'] = report.rcp_no
                    prepared_df['report_nm'] = report.report_nm
                    prepared_df['rcept_dt'] = report.rcept_dt
                    prepared_df['available_at'] = report.rcept_dt
                    prepared_df['stock_code'] = report.stock_code
                    prepared_df['corp_code'] = report.corp_code
                    prepared_df['corp_name'] = report.corp_name
                    return {f"{f}": prepared_df}
        except NotFoundConsolidated as e:
            logger.info('Warning: NotFoundConsolidated')
            return {}
        except NoDataReceived as e:
            logger.info('Warning: NoDataReceived')
            return {}
        except Exception as ex:
            logger.info(f'Warning[Exception]: {ex}')
            return {}
        return {}

    def dart_fs_by_corp(self, from_date: str, to_date: str):
        try:
            from_date = from_date if from_date and isinstance(from_date, str) else self._today()
            to_date = to_date if to_date and isinstance(to_date, str) else self._today()

            df_bs = DataFrame() # 연결재무상태표
            df_is = DataFrame() # 연결손익계산서
            df_cis = DataFrame() # 연결포괄손익계산서
            df_cf = DataFrame() # 현금흐름표
            
            # NOTE 하위 기업 
            under_20 = pd.read_csv('sorted_result_under_50.csv')
            ticker_list = under_20['티커']
            # 모든 상장된 기업 리스트 불러오기
            corp_list = self.dart.get_corp_list()

            for idx, ticker in enumerate(ticker_list):
                corp = corp_list.find_by_stock_code(ticker)
                logger.info(f'[{idx}/{len(ticker_list)}]corp: {corp}, from_date:{from_date}, to_date:{to_date}')
                if corp:
                    # samsung = corp_list.find_by_corp_name(corp_code=corp_code)
                    # fs = samsung.extract_fs(bgn_de='20120101') 와 동일
                    try:
                        extract_fs = self.dart.fs.extract(corp_code=corp.corp_code, bgn_de=from_date)
                        
                        df_bs = pd.concat([df_bs, extract_fs['bs']], ignore_index=True)
                        df_is = pd.concat([df_is, extract_fs['is']], ignore_index=True)
                        df_cis = pd.concat([df_cis, extract_fs['cis']], ignore_index=True)
                        df_cf = pd.concat([df_cf, extract_fs['cf']], ignore_index=True)
                    except NotFoundConsolidated as e:
                        logger.info('Warning: NotFoundConsolidated')
                    except NoDataReceived as e:
                        logger.info('Warning: NoDataReceived')
                    except Exception as ex:
                        logger.info(f'Warning[Exception]: {ex}')

            df_bs.to_csv(f'연결재무상태표_{from_date}_{to_date}.csv')
            df_is.to_csv(f'연결손익계산서_{from_date}_{to_date}.csv')
            df_cis.to_csv(f'연결포괄손익계산서_{from_date}_{to_date}.csv')
            df_cf.to_csv(f'현금흐름표_{from_date}_{to_date}.csv')
        except OverQueryLimit as e:
            logger.info(f'Warning[OverQueryLimit]: {e}')
            self.set_next_api_key()
            return self.dart_fs_by_corp(from_date, to_date)
        return


    def dart_fs_by_day(self, from_date: str, to_date: str):
        try:
            # 2012년 01월 01일 부터 연결재무제표 검색
            # fs = samsung.extract_fs(bgn_de='20200101') 와 동일
            # corp_code = '00126380'
            from_date = from_date if from_date and isinstance(from_date, str) else self._today()
            to_date = to_date if to_date and isinstance(to_date, str) else self._today()

            df_bs = DataFrame() # 연결재무상태표
            df_is = DataFrame() # 연결손익계산서
            df_cis = DataFrame() # 연결포괄손익계산서
            df_cf = DataFrame() # 현금흐름표
            
            bgn_date = ''
            end_date = from_date
            while end_date <= to_date:
                bgn_date = end_date
                end_date = (dt.strptime(bgn_date, "%Y%m%d") + timedelta(weeks=4)).strftime("%Y%m%d")
                logger.info(f"dart_all_fs - bgn_date[{bgn_date}] ~ end_date[{end_date}]")

                for idx in ('A001', 'A002', 'A003'): # 년, 반기, 분기
                    # 한달 씩 데이터를 수집한다.
                    reports = self.dart.filings.search(bgn_de=bgn_date, end_de=end_date,
                                                pblntf_detail_ty=idx, page_count=100,
                                                last_reprt_at="N")
                    
                    for _ in range(len(reports)):
                        report = reports.pop(0)
                        extract_fs = self._get_fs(report)
                        if 'bs' in extract_fs.keys():
                            df_bs = pd.concat([df_bs, extract_fs['bs']], ignore_index=True)
                        if 'is' in extract_fs.keys():
                            df_is = pd.concat([df_is, extract_fs['is']], ignore_index=True)
                        if 'cis' in extract_fs.keys():
                            df_cis = pd.concat([df_cis, extract_fs['cis']], ignore_index=True)
                        if 'cf' in extract_fs.keys():
                            df_cf = pd.concat([df_cf, extract_fs['cf']], ignore_index=True)
                    sleep(1)

            df_bs.to_csv(f'연결재무상태표_{from_date}_{to_date}.csv')
            df_is.to_csv(f'연결손익계산서_{from_date}_{to_date}.csv')
            df_cis.to_csv(f'연결포괄손익계산서_{from_date}_{to_date}.csv')
            df_cf.to_csv(f'현금흐름표_{from_date}_{to_date}.csv')
        except OverQueryLimit as e:
            logger.info(f'Warning[OverQueryLimit]: {e}')
            self.set_next_api_key()
            return self.dart_fs_by_day(bgn_date, end_date)
        return 


class KrxCollector():
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