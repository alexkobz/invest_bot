import numpy as np
import pandas as pd

from strategy.Strategy import Strategy


class MovingAverageStrategy(Strategy):
    """
    A trading strategy based on moving averages.
    The strategy generates buy/sell signals based on the crossover of multiple moving averages.
    When a shorter-term moving average crosses above a longer-term moving average, it generates a buy signal.
    Conversely, when the shorter-term moving average crosses below the longer-term moving average, it generates a sell signal.
    """

    def __init__(self, df, windows: list[int]):
        super().__init__(df)
        self.windows = sorted(list(windows))
        self.windows_combination = []
        self.modify_df()

    def modify_df(self):
        """
        Modify the DataFrame to include moving averages.
        """
        from itertools import combinations
        for window in self.windows:
            if not window > 0 and not isinstance(window, int):
                raise ValueError("Window1 must be a positive integer.")
            if f'MA{window}' not in self.df.columns:
                self.df[f'MA{window}'] = self.prices.rolling(window=window).mean()
        self.windows_combination = list(combinations(self.windows, 2))

    def run(self, *args, **kwargs) -> pd.DataFrame:
        """
        Run the strategy by calculating the position based on moving averages.
        The position is 1 when MA1 is greater than MA2, and -1 otherwise.
        It calculates the cumulative returns of the strategy and compares it to the market returns.
        """
        for window1, window2 in self.windows_combination:
            self.df[f'position_MA{window1}_{window2}'] = np.where(self.df[f'MA{window1}'] > self.df[f'MA{window2}'], 1, -1)
            self.df[f'strategy_return_MA{window1}_{window2}'] = self.df[f'position_MA{window1}_{window2}'].shift(1) * self.df['log_returns1']
            self.df[f'cumreturns_MA{window1}_{window2}'] = self.df['log_returns1'].cumsum().apply(np.exp)
            self.df[f'cumstrategy_MA{window1}_{window2}'] = self.df[f'strategy_return_MA{window1}_{window2}'].cumsum().apply(np.exp)
        return self.df

    def plot(self, secid: str, window1: int, window2: int) -> None:
        """
        Plot the cumulative returns of the strategy and the market.
        """
        title = f'MA{window1}, MA{window2}'
        self.df.loc[self.df['secid']==secid, [f'cumreturns_MA{window1}_{window2}', f'cumstrategy_MA{window1}_{window2}']].plot(
            title=title,
            figsize=(10, 6))
