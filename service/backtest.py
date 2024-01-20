from datetime import datetime as dt, timedelta
from matplotlib import pyplot as plt
import pandas as pd
from service.strategy import QuantStragegy
from logger import logger

class Backtest(object):
    strategy: QuantStragegy


    def __init__(self, stragery: QuantStragegy, period_data: pd.DataFrame):
        self.stragery = stragery
        self.period_data = period_data

    def show_chart(self, data):
        # 데이터프레임으로 변환
        df_one_month = pd.DataFrame(data)

        # 'date' 컬럼을 datetime 형식으로 변환
        df_one_month['date'] = pd.to_datetime(df_one_month['date'], format='%Y%m%d')

        # 그래프 그리기
        plt.figure(figsize=(10, 6))
        plt.plot(df_one_month['date'], df_one_month['amount'], marker='o', linestyle='-')
        plt.title('Amount by Date (One Month)')
        plt.xlabel('Date')
        plt.ylabel('Amount')
        plt.xticks(rotation=45)
        plt.tight_layout()  # 레이아웃 조정
        plt.grid(True)
        plt.show()

    def execute(self, start_date: dt, end_date: dt, rebalancing_date: list[str], extract_count=20, set_amount = 20000000):
        """
        필요한 날짜에 대한 정보를 받아서 초기 금액을 가지고 backtest를 진행한다.
        """
        # 테스트를 진행할 특정 기간을 추출한다.
        period_data = self.period_data[(self.period_data['trade_date'] >= int(start_date.strftime("%Y%m%d")))
                                       & (self.period_data['trade_date'] <= int(end_date.strftime("%Y%m%d")))]

        market_date = period_data['trade_date'].drop_duplicates().astype(str).tolist()

        logger.info(f"market_date: {market_date} ({len(market_date)} count)")
        logger.info(f"rebalancing_date: {rebalancing_date} {len(rebalancing_date)}")

        history_bucket = pd.DataFrame()
        bought_bucket = pd.DataFrame()
        daily_total_amount = []

        for idx, date in enumerate(market_date):
            # 리벨런싱인 날의 동작
            if date in rebalancing_date:
                logger.info(f'rebalancing for {date}')
                extract_stock = self.stragery.extract_stock(target_date=str(date), extract_count=extract_count)

                # 두번째 리밸런싱부터 전체 잔액을 구한다.
                if idx > 0:
                    set_amount = bought_bucket['buy_amount'].sum()

                extract_stock['buy_count'] = set_amount/extract_count/extract_stock['종가']
                extract_stock['buy_count'] = extract_stock['buy_count'].astype(int)
                extract_stock['buy_amount'] = extract_stock['buy_count']*extract_stock['종가']
                extract_stock['buy_date'] = str(date)
                
                bought_bucket = extract_stock[['티커', 'buy_date', '종가', 'buy_count', 'buy_amount']]
                history_bucket = pd.concat([history_bucket, bought_bucket], ignore_index=True)
                daily_total_amount.append({'date':str(date), 'amount':extract_stock['buy_amount'].sum()})
                # next_day로 넘어간다.
                continue
            
            stock = {'티커':[], 'buy_date':[],  'buy_count':[], '종가':[], 'buy_amount':[]}
            for row in bought_bucket.itertuples():
                stock['티커'].append(row.티커)
                stock['buy_date'].append(date)
                stock['buy_count'].append(row.buy_count)
                close_price = self.period_data[(self.period_data['trade_date'] == int(date)) & (self.period_data['티커'] == row.티커)]['종가']
                stock['종가'].append(int(close_price.iloc[0])) if len(close_price) >= 1 else logger.info(f'close_price is None: {close_price}[{type(close_price)}]')
                stock['buy_amount'].append(row.buy_count * (int(close_price.iloc[0]) if len(close_price) >= 1 else 0))
            bought_bucket = pd.DataFrame(stock)
            daily_total_amount.append({'date':str(date), 'amount':sum(stock['buy_amount'])})
            history_bucket = pd.concat([history_bucket, bought_bucket], ignore_index=True)
        
        history_bucket.to_csv('backtest_history_bucket.csv')
        logger.info(f"final amount: {set_amount}")
        
        # NOTE daily_total_amount 그래프를 그리면 전략의 수익률 그래프
        logger.info(f"daily_total_amount: {daily_total_amount}")
        self.show_chart(daily_total_amount)
        
        return history_bucket
    