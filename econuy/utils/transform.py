import pandas as pd
import numpy as np

from econuy.utils.exceptions import InvalidTransformation


def error_handler(df: pd.DataFrame, errors: str, msg: str = None) -> pd.DataFrame:
    if errors == "coerce":
        return pd.DataFrame(data=np.nan, index=df.index, columns=df.columns)
    elif errors == "ignore":
        return df
    elif errors == "raise":
        if msg is None:
            msg = ""
        raise InvalidTransformation(msg)
