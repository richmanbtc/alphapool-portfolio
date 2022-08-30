import pandas as pd
import numpy as np


class EqualWeight:
    def __init__(self):
        pass

    def get_weights(self, df):
        model_ids = df.columns
        model_count = model_ids.shape[0]

        return pd.DataFrame(
            np.ones((model_count, 1)) / (1e-37 + model_count),
            index=model_ids,
            columns=["weight"],
        )
