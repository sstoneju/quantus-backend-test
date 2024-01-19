from datetime import datetime
from matplotlib.pyplot import axis
from numpy import dtype, float64, int64
import pandas as pd
from loguru import logger


class Transform(object):
    """ 수집한 데이터를 한번 더 가공처리를 해서 사용할 수 있는 데이터로 만든다.
    market_cap + 재무재표 = PER, PSR, POR, PGPR, GPA
    
    """
    def __init__(self):
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
        merged_data_with_date.to_csv("ffill_fs_bs.csv")
        logger.info(merged_data_with_date)


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
    

    def bind_for_strategy(self, market = "market_cap_by_ticker_kospi_2022.csv", cis_file = 'clean_df_cis.csv', is_file = 'clean_df_is.csv', bs_file = "clean_fs_bs.csv"):
        market_data = pd.read_csv(market, dtype={'stock_code': str}, index_col=False)
        financial_cis_data = pd.read_csv(cis_file, dtype={'stock_code': str}, index_col=False)
        financial_is_data = pd.read_csv(is_file, dtype={'stock_code': str}, index_col=False)
        financial_bs_data = pd.read_csv(bs_file, dtype={'stock_code': str}, index_col=False)
        
        # 주가 데이터 로드 (예: price_data.csv)
        print(financial_cis_data)
        financial_cis_data = self.preprocess_financial_data(financial_cis_data)
        financial_is_data = self.preprocess_financial_data(financial_is_data)
        financial_bs_data = self.preprocess_financial_bs_data(financial_bs_data)


        revenue_date = self.calculate_quarter_difference(financial_cis_data, target_year=2022, target_col='매출원가')

        net_income_date = self.calculate_quarter_difference(financial_cis_data, target_year=2022, target_col='당기순이익(손실)') 
        net_income_date_2 = self.calculate_quarter_difference(financial_is_data, target_year=2022, target_col='당기순이익(손실)')
        net_income_date = net_income_date.merge(net_income_date_2, on=['year', 'quarter'], how='left')
        # net_income_date.to_csv("net_income_date.csv")
        # NOTE 이슈: 당기순익이 표시되고 있는 회사의 수가 85개 밖에 안됨.
        # print(len(net_income_date.dropna(axis=1).columns.to_list()))

        total_assets_data = self.calculate_quarter_difference_for_bs(financial_bs_data, target_year=2022, target_col='자산총계') # PBR
        total_capital_data = self.calculate_quarter_difference_for_bs(financial_bs_data, target_year=2022, target_col='자본총계') # ROE
        total_assets_data.to_csv('total_assets_data.csv')
        total_capital_data.to_csv('total_capital_data.csv')

        # market_cap과 합치는 작업 진행.
        net_income = self.merge_with_price_data(net_income_date, market_data, 'net_income')
        total_assets = self.merge_with_price_data(total_assets_data, net_income, 'total_assets')
        total_capital = self.merge_with_price_data(total_capital_data, total_assets, 'total_capital')
        # NOTE 이슈: 당기순이익, 자산총계가 모두 나오는 데이터가 없음.
        # total_capital.to_csv("total_capital.csv")
        total_capital.to_csv("total_capital.csv")
    

        # NOTE PER, PBR 계산
        # monthly_financial_data['PER'] = monthly_price_data['close_price'] / monthly_financial_data['earnings_per_share']
        # monthly_financial_data['PBR'] = monthly_price_data['close_price'] / monthly_financial_data['book_value_per_share']
        # print(monthly_financial_data)


    def preprocess_financial_data(self, financial_data:pd.DataFrame):
        """재무 데이터 전처리."""
        print('preprocess_financial_data ...')
        financial_data['date'] = pd.to_datetime(financial_data['rcept_dt'], format='%Y%m%d')
        financial_data[['fs_start_date', 'fs_end_date']] = financial_data['fs_date'].str.split('-', expand=True)
        financial_data['fs_start_date'] = pd.to_datetime(financial_data['fs_start_date'])
        financial_data['fs_end_date'] = pd.to_datetime(financial_data['fs_end_date'])
        # NOTE 이 부분을 available_at으로 바꿀 수 있음
        financial_data['year'] = financial_data['fs_start_date'].dt.year
        financial_data['quarter'] = financial_data['fs_start_date'].dt.quarter
        financial_data['end_quarter'] = financial_data['fs_end_date'].dt.quarter
        return financial_data


    def preprocess_financial_bs_data(self, financial_data:pd.DataFrame):
        """재무상태표 데이터 전처리"""
        print('preprocess_financial_bs_data ...')
        financial_data['date'] = pd.to_datetime(financial_data['rcept_dt'], format='%Y%m%d')
        financial_data['fs_start_date'] = pd.to_datetime(financial_data['fs_date'], format='%Y%m%d')
        # NOTE 이 부분을 available_at으로 바꿀 수 있음
        financial_data['year'] = financial_data['fs_start_date'].dt.year
        financial_data['quarter'] = financial_data['fs_start_date'].dt.quarter
        financial_data['end_quarter'] = financial_data['fs_start_date'].dt.quarter
        return financial_data


    def calculate_quarter_difference(self, financial_data:pd.DataFrame, target_year: int, target_col: str = '매출원가') -> pd.DataFrame:
        """분기 차이 계산 및 필터링된 데이터 저장."""
        print("calculate_quarter_difference ...")
        # NOTE 이 부분을 available_at으로 바꿀 수 있음
        pivot_data = financial_data[financial_data['label_ko'] == target_col].pivot_table(
            index=['stock_code'], 
            columns=['year', 'quarter', 'end_quarter'],
            values='amount', 
            aggfunc='first'
        ).swaplevel(0, 2, axis=1)
        print(pivot_data)
        # 4분기 데이터 생성
        data_to_be_subtracted = pivot_data.loc[:, (4, 1, target_year)]
        data_to_subtract = pivot_data.loc[:, (3, 1, target_year)]
        pivot_data[(4, 4, target_year)] = data_to_be_subtracted.values - data_to_subtract.values

        # 해당하는 년도와 
        mask_year = pivot_data.columns.get_level_values('year') == target_year
        mask_quarter = pivot_data.columns.get_level_values('end_quarter') == pivot_data.columns.get_level_values('quarter')
        combined_mask = mask_year & mask_quarter
        filtered_df = pivot_data.loc[:, combined_mask].transpose().reset_index()
        return filtered_df
    

    def calculate_quarter_difference_for_bs(self, financial_data:pd.DataFrame, target_year: int, target_col: str = '매출원가') -> pd.DataFrame:
        """분기 차이 계산 및 필터링된 데이터 저장."""
        print("calculate_quarter_difference ...")
        # NOTE 이 부분을 available_at으로 바꿀 수 있음
        pivot_data = financial_data[financial_data['label_ko'] == target_col].pivot_table(
            index=['stock_code'], 
            columns=['year', 'quarter', 'end_quarter'],
            values='amount', 
            aggfunc='first'
        ).swaplevel(0, 2, axis=1)
        
        # 해당하는 년도와 
        mask_year = pivot_data.columns.get_level_values('year') == target_year
        mask_quarter = pivot_data.columns.get_level_values('end_quarter') == pivot_data.columns.get_level_values('quarter')
        combined_mask = mask_year & mask_quarter
        filtered_df = pivot_data.loc[:, combined_mask].transpose().reset_index()
        return filtered_df


    def merge_with_price_data(self, financial_data:pd.DataFrame, market_cap:pd.DataFrame, col_name: str = 'revenue'):
        """주가 데이터와 재무 데이터 병합."""
        bind_market = pd.DataFrame()
        if "year" not in market_cap.columns.to_list() and "quarter" not in market_cap.columns.to_list():
            market_cap['trade_date'] = pd.to_datetime(market_cap['trade_date'], format='%Y%m%d')
            # NOTE 이 부분을 trade_date으로 바꾸고 available_at으로 머지한다면?
            market_cap['year'] = market_cap['trade_date'].dt.year
            market_cap['quarter'] = market_cap['trade_date'].dt.quarter

        stock_list_2023 = market_cap['티커'].drop_duplicates().to_list()
        print(f"티커 {len(stock_list_2023)}개")

        for ticker in stock_list_2023:
            ticker_price_data = market_cap[market_cap['티커'] == ticker]
            
            if ticker not in financial_data.columns.to_list():
                bind_market = pd.concat([bind_market, ticker_price_data], ignore_index=True)
                continue
            
            arrange_data = financial_data.loc[:, ['quarter', 'year', ticker]].rename(columns={ticker: f'{col_name}'})
            merged_data = ticker_price_data.merge(arrange_data, on=['year', 'quarter'], how='left')
            bind_market = pd.concat([bind_market, merged_data], ignore_index=True)
        
        return bind_market