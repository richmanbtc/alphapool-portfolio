import pandas as pd
import numpy as np
from ..equal_weight import EqualWeight


class UpBase:
    def __init__(self, algo):
        self._algo = algo

    def get_weights(self, df):
        if df.shape[1] == 1:
            return EqualWeight().get_weights(df)

        df = np.maximum(0.1, 1 + df).cumprod()
        result = self._algo.run(df)
        weights = result._B

        return pd.DataFrame(
            weights.iloc[-1].values.reshape(-1, 1),
            index=weights.columns,
            columns=["weight"],
        )
