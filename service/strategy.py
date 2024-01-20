from operator import indexOf
import os
import numpy as np
import pandas as pd
from logger import logger


class QuantStragegy(object):
    """
    데이터를 지정하고, 팩터들을 설정하고, sorting 후에 종목을 추출한다.
    """
    factor_pack: list
    kind_of_factor = ['per', 'pbr', 'roe', 'roa', 'debt_rate']

    def __init__(self):
        # NOTE 수집 데이터가 완벽하지 않으니 mock으로 작업을 진행한다.
        self.mock_data = pd.read_csv('market_cap_by_ticker_kospi_2023.csv', index_col=[0])
        return

    def set_factor(self, factor_name: str):
        """
        팩터를 지정해준다.
        """
        if factor_name in self.kind_of_factor:
            logger.info(f"Can't set the factor: {factor_name}")
            return False
        logger.info(f"Set factor: {factor_name}")
        self.factor_pack.append(factor_name)
        return True
    
    def _attach_mock_factor(self, stock_list: pd.DataFrame):
        logger.info('Attaching temp data, use for factor data')
        # 데이터 프레임의 행과 열의 수를 설정
        num_rows = len(stock_list)

        # 정수 값을 생성하려면 np.random.randint를 사용
        # 실수 값을 생성하려면 np.random.uniform을 사용
        per_series = pd.Series(np.random.uniform(low=2, high=100, size=(num_rows)))
        pbr_series = pd.Series(np.random.uniform(low=0.5, high=5, size=(num_rows)))
        roe_series = pd.Series(np.random.uniform(low=1, high=20, size=(num_rows)))
        roa_series = pd.Series(np.random.uniform(low=1, high=20, size=(num_rows)))
        debt_rate_series = pd.Series(np.random.uniform(low=20, high=150, size=(num_rows)))
        
        stock_list['per'] = per_series
        stock_list['pbr'] = pbr_series
        stock_list['roe'] = roe_series
        stock_list['roa'] = roa_series
        stock_list['debt_rate'] = debt_rate_series

        # logger.info(f"\n{stock_list}")

        return stock_list
    
    def _get_per_score(self, stock_list: pd.DataFrame):
        # per 값이 낮으면 좋은 기업이기 때문에 rank를 높게 매긴다.
        df_sorted = stock_list.sort_values(by='per', ascending=False) # 낮은 값에 높은 값을 주기 위해 정렬
        df_sorted['per_score'] = range(1, len(df_sorted) + 1)
        return df_sorted
    
    def _get_pbr_score(self, stock_list: pd.DataFrame):
        df_sorted = stock_list.sort_values(by='pbr', ascending=False) # 낮은 값에 높은 값을 주기 위해 정렬
        df_sorted['pbr_score'] = range(1, len(df_sorted) + 1)
        return df_sorted
    
    def _get_roe_score(self,stock_list: pd.DataFrame):
        df_sorted = stock_list.sort_values(by='roe', ascending=True) # 높은 값에 높은 값을 주기 위해 정렬
        df_sorted['roe_score'] = range(1, len(df_sorted) + 1)
        return df_sorted
    
    def _get_roa_score(self, stock_list: pd.DataFrame):
        df_sorted = stock_list.sort_values(by='roa', ascending=True) # 낮은 값에 높은 값을 주기 위해 정렬
        df_sorted['roa_score'] = range(1, len(df_sorted) + 1)
        return df_sorted
    
    def _get_debt_rate_score(self, stock_list: pd.DataFrame):
        df_sorted = stock_list.sort_values(by='debt_rate', ascending=False) # 낮은 값에 높은 값을 주기 위해 정렬
        df_sorted['debt_rate_score'] = range(1, len(df_sorted) + 1)
        return df_sorted

    def extract_stock(self, target_date_stock: pd.DataFrame=None, extract_count: int = 20, target_date: str='') -> pd.DataFrame:
        """
        fator_pack에 들어가게 된 factor를 기반으로 stock_list에 있는 주식의 점수를 계산하여 컬럼에 추가한다.
        추가한 점수를 total_score로 계산해서 sorting 후 추출 (반환값은 ticker, stock_name)
        eg) per_score, pbr_score
        """
        target_date_stock = self.mock_data[self.mock_data['trade_date'] == int(target_date)].reset_index(drop=True)        
        # NOTE 팩터를 임의의 데이터로 채워준다. 현 상태로는 고정값의 factor로 테스트를 하지 못함. 추출해서 backtest 동작을 테스트를 할 순 있다.
        target_date_stock = self._attach_mock_factor(target_date_stock)
    
        # NOTE 클래스의 외부에서 set_factor를 이용해 설정해준다.
        self.factor_pack = ['per', 'pbr', 'roe', 'roa', 'debt_rate']
        self.factor_scores = ['per_score', 'pbr_score', 'roe_score', 'roa_score', 'debt_rate_score']

        cal_score_stock = target_date_stock
        for factor in self.factor_pack:
            if factor == 'per':
                cal_score_stock = self._get_per_score(cal_score_stock)
            if factor == 'pbr':
                cal_score_stock = self._get_pbr_score(cal_score_stock)
            if factor == 'roe':
                cal_score_stock = self._get_roe_score(cal_score_stock)
            if factor == 'roa':
                cal_score_stock = self._get_roa_score(cal_score_stock)
            if factor == 'debt_rate':
                cal_score_stock = self._get_debt_rate_score(cal_score_stock)
        
        cal_score_stock['total_score'] = cal_score_stock[self.factor_scores].sum(axis=1)

        return cal_score_stock.sort_values(by='total_score', ascending=False).reset_index(drop=True).head(extract_count)