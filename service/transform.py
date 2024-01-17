from datetime import datetime
from numpy import float64, int64
import pandas as pd
from loguru import logger


class Transform(object):
    """ 수집한 데이터를 한번 더 가공처리를 해서 사용할 수 있는 데이터로 만든다.
    market_cap + 재무재표 = PER, PSR, POR, PGPR, GPA
    
    """
    def __init__(self, stock_ohlcv:str, market_cap:str):
        self.stock_ohlcv = pd.read_csv(stock_ohlcv)
        self.market_cap = pd.read_csv(market_cap)
        return
    
    def _parse_str_date(self, date_string:str):
        """
        YYYYMMDD의 string 타입의 날짜를 넣으면 DataFrame에서 사용하는 date format으로 parse한다.
        """
        parsed_date = datetime.strptime(date_string, '%Y%m%d').strftime('%Y-%m-%d')
        return parsed_date

    def ffill_fs_bs(self, fs_bs_csv:str, from_date:str, to_date:str):
        """
        재무재표가 미래참조가 일어나지 않게 trade_date를 available_at으로 forward fill 진행한다.
        그렇게 되면 원하는 날짜에 어떤 재무재표 데이터를 참조할 수 있는지 간단하게 알 수 있다.
        """
        df_bs = pd.read_csv(fs_bs_csv if '.csv' in fs_bs_csv else f'{fs_bs_csv}.csv')
        # NOTE amount가 없는 데이터는 사용하지 않는다.

        # NOTE label_ko를 남긴다.
        dates = pd.date_range(start=self._parse_str_date(from_date), end=self._parse_str_date(to_date), freq='D')
        daily_data_with_date = pd.DataFrame({'trade_date': dates})
        daily_data_with_date['trade_date'] = daily_data_with_date['trade_date'].astype(str)
        logger.info(daily_data_with_date['trade_date'])

        df_bs['available_at'] = df_bs['available_at'].apply(lambda x: pd.to_datetime(x, format='%Y%m%d').strftime('%Y-%m-%d'))
        
        df_bs = df_bs.drop(columns=['Unnamed: 0', 'concept_id', 'label_en', 'class0', 'class1', 'class2', 'class3'])
        clean_df = df_bs.dropna(subset=['amount'])
        clean_df = clean_df[clean_df['fs_date'].str.len() < 20]
        clean_df.to_csv("clean_fs_bs.csv")

        # 가상의 재무재표 데이터를 날짜별 데이터 프레임에 병합
        merged_data_with_date = daily_data_with_date.merge(clean_df, left_on='trade_date', right_on='available_at', how='left')

        # 포워드 필로 빈 값을 채움
        # filled_data_with_date = merged_data_with_date.fillna(method='ffill')
        merged_data_with_date.to_csv("ffill_fs_bs.csv")
        logger.info(merged_data_with_date)

        return

    def ffill_fs_is(self, fs_is_csv:str, from_date:str, to_date:str):
        """
        손익계산서가 미래참조가 일어나지 않게 trade_date를 available_at으로 forward fill 진행한다.
        그렇게 되면 원하는 날짜에 어떤 재무재표 데이터를 참조할 수 있는지 간단하게 알 수 있다.
        """
        df_bs = pd.read_csv(fs_is_csv if '.csv' in fs_is_csv else f'{fs_is_csv}.csv', dtype={'stock_code': str})
        # NOTE amount가 없는 데이터는 사용하지 않는다.

        # NOTE label_ko를 남긴다.
        dates = pd.date_range(start=self._parse_str_date(from_date), end=self._parse_str_date(to_date), freq='D')
        daily_data_with_date = pd.DataFrame({'trade_date': dates})
        daily_data_with_date['trade_date'] = daily_data_with_date['trade_date'].astype(str)
        logger.info(daily_data_with_date['trade_date'])

        df_bs['available_at'] = df_bs['available_at'].apply(lambda x: pd.to_datetime(x, format='%Y%m%d').strftime('%Y-%m-%d'))
        logger.info(df_bs['available_at'])

        # 사용하지 않는 컬럼을 제거하고 amount가 빠진 값을 수정한다.
        # clean_df = df_bs[df_bs['amount'].astype(str).str.strip() != '']
        df_bs['stock_code'] = df_bs['stock_code'].astype(str)
        df_bs = df_bs.drop(columns=['Unnamed: 0', 'concept_id', 'label_en', 'class0', 'class1'])
        clean_df = df_bs.dropna(subset=['amount'])
        clean_df = clean_df[clean_df['fs_date'].str.len() < 20]
        clean_df = clean_df[clean_df['report_nm'].str.contains('분기')]

        clean_df.to_csv("clean_df.csv")

        # 가상의 재무재표 데이터를 날짜별 데이터 프레임에 병합
        merged_data_with_date = daily_data_with_date.merge(clean_df, left_on='trade_date', right_on='available_at', how='right')

        # 포워드 필로 빈 값을 채움
        filled_data_with_date = merged_data_with_date.fillna(method='ffill')
        filled_data_with_date.to_csv("ffill_fs_is.csv")
        logger.info(filled_data_with_date)

        return
