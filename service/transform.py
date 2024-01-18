from datetime import datetime
from matplotlib.pyplot import axis
from numpy import float64, int64
import pandas as pd
from loguru import logger


class Transform(object):
    """ 수집한 데이터를 한번 더 가공처리를 해서 사용할 수 있는 데이터로 만든다.
    market_cap + 재무재표 = PER, PSR, POR, PGPR, GPA
    
    """
    def __init__(self, stock_ohlcv:str, market_cap:str):
        self.stock_ohlcv = pd.read_csv(stock_ohlcv)
        self.market_cap = pd.read_csv(market_cap, dtype={"티커":str}, index_col=0)
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
        df_bs = df_bs.drop(columns=['Unnamed: 0', 'concept_id', 'label_en', 'class0', 'class1', 'class2', 'class3','class4','comment'])
        clean_df = df_bs.dropna(subset=['amount'])

        clean_df.to_csv("clean_df_is.csv")

        # 가상의 재무재표 데이터를 날짜별 데이터 프레임에 병합
        merged_data_with_date = daily_data_with_date.merge(clean_df, left_on='trade_date', right_on='available_at', how='right')

        # 포워드 필로 빈 값을 채움
        filled_data_with_date = merged_data_with_date.fillna(method='ffill')
        filled_data_with_date.to_csv("ffill_fs_is.csv")
        logger.info(filled_data_with_date)

        return
    
    def ffill_fs_cis(self, fs_is_csv:str, from_date:str, to_date:str):
        """
        포괄손익계산서가 미래참조가 일어나지 않게 trade_date를 available_at으로 forward fill 진행한다.
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
        df_bs = df_bs.drop(columns=['Unnamed: 0', 'concept_id', 'label_en', 'class0', 'class1', 'class2'])
        clean_df = df_bs.dropna(subset=['amount'])
        clean_df = clean_df[clean_df['fs_date'].str.len() < 20]

        clean_df.to_csv("clean_df_cis.csv")

        # 가상의 재무재표 데이터를 날짜별 데이터 프레임에 병합
        merged_data_with_date = daily_data_with_date.merge(clean_df, left_on='trade_date', right_on='available_at', how='right')

        # 포워드 필로 빈 값을 채움
        filled_data_with_date = merged_data_with_date.fillna(method='ffill')
        filled_data_with_date.to_csv("ffill_fs_cis.csv")
        logger.info(filled_data_with_date)

        return

    def bind_for_strategy(self):
        # 주가 데이터 로드 (예: price_data.csv)

        price_data = self.market_cap
        price_data['date'] = price_data['trade_date'].apply(lambda x: pd.to_datetime(x, format='%Y%m%d'))
        # price_data = price_data.drop(columns='trade_date')

        # 재무제표 데이터 로드 (예: financial_data.csv)
        financial_data = pd.read_csv('clean_df_is.csv', dtype={'stock_code': str})
        financial_data['date'] = financial_data['rcept_dt'].apply(lambda x: pd.to_datetime(x, format='%Y%m%d'))
        financial_data[['fs_start_date', 'fs_end_date']] = financial_data['fs_date'].str.split('-', expand=True)
        
        # 시작 날짜와 종료 날짜를 날짜 형식으로 변환
        financial_data['fs_start_date'] = pd.to_datetime(financial_data['fs_start_date'])
        financial_data['fs_end_date'] = pd.to_datetime(financial_data['fs_end_date'])

        # 연도와 분기를 나타내는 열 생성
        financial_data['year'] = financial_data['fs_start_date'].dt.year
        financial_data['quarter'] = financial_data['fs_start_date'].dt.quarter
        financial_data['end_quarter'] = financial_data['fs_end_date'].dt.quarter
        # print(financial_data.head())
        financial_data.to_csv('financial_data_20240117.csv', index=False)
        
        # 매달 말일 데이터로 리샘플링

        stock_list_2023 = price_data['티커'].drop_duplicates().to_list()
        print(f"티커 {len(stock_list_2023)}개")

        def process_row(row):
            aaa = financial_data[(financial_data['year']==row['year']) & (financial_data['quarter']==row['quarter']) & (financial_data['end_quarter']==row['quarter']) & (financial_data['stock_code']==row['티커'])]
            # for k, v in aaa:
            #     row[k['label_ko']] = aaa['amount']
            print(row)

        for ticker in stock_list_2023:
            monthly_price_data = price_data[price_data['티커'] == ticker].resample('M', on='date').last()
            monthly_price_data['trade_date'] = monthly_price_data['trade_date'].apply(lambda x: pd.to_datetime(x, format='%Y%m%d'))
            # print(monthly_price_data)
            monthly_price_data['year'] = monthly_price_data['trade_date'].dt.year
            monthly_price_data['quarter'] = monthly_price_data['trade_date'].dt.quarter


            aaa = monthly_price_data.apply(process_row,axis=1)
            print(aaa)


            break

        # # # 분기별 데이터를 매달 데이터와 동기화
        # monthly_financial_data = financial_data.resample('M', on='fs_end_date')

        # # PER, PBR 계산
        # monthly_financial_data['PER'] = monthly_price_data['close_price'] / monthly_financial_data['earnings_per_share']
        # monthly_financial_data['PBR'] = monthly_price_data['close_price'] / monthly_financial_data['book_value_per_share']
        # print(monthly_financial_data)
