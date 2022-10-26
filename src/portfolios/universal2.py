import numpy as np
import pandas as pd


class Universal2:
    def __init__(self, max_leverage=5):
        self.max_leverage = max_leverage

    def get_weights(self, df):
        weight_count = 10000
        max_leverage = self.max_leverage
        min_return = 0.01

        model_ids = df.columns
        model_count = model_ids.shape[0]
        rs = np.random.RandomState(1)

        sum_weights = np.zeros(model_count)
        sum_wealth = 0.0
        for i in range(weight_count):
            weights = rs.dirichlet(np.ones(model_count), 1).flatten()
            # weights *= 2 * rs.randint(0, 2, model_count) - 1
            weights *= rs.rand() * max_leverage
            ret = np.sum(df.values * weights.reshape(1, -1), axis=1)
            ret = np.maximum(min_return, 1 + ret)
            wealth = np.cumprod(ret)[-1]
            sum_weights += weights * wealth
            sum_wealth += wealth

        return pd.DataFrame(
            sum_weights / sum_wealth,
            index=model_ids,
            columns=["weight"],
        )
