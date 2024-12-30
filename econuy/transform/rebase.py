from typing import Union, Tuple
from datetime import datetime

import pandas as pd


def _rebase(
    data: pd.DataFrame,
    metadata: "Metadata",  # type: ignore # noqa: F821
    start_date: Union[str, datetime],
    end_date: Union[str, datetime, None] = None,
    base: float = 100.0,
) -> Tuple[pd.DataFrame, "Metadata"]:  # type: ignore # noqa: F821
    metadata = metadata.copy()
    if end_date is None:
        m_end = None
        start_date = data.iloc[
            data.index.get_indexer([start_date], method="nearest")
        ].index[0]
        output = data.apply(lambda x: x / x.loc[start_date] * base)
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if not isinstance(base, int):
            if base.is_integer():
                base = int(base)
        m_start = start_date.strftime("%Y-%m")
        metadata.update_dataset_metadata({"unit": f"{m_start}={base}"})

    else:
        output = data.apply(lambda x: x / x[start_date:end_date].mean() * base)
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        m_start = start_date.strftime("%Y-%m")
        m_end = end_date.strftime("%Y-%m")
        if not isinstance(base, int):
            if base.is_integer():
                base = int(base)
        if m_start == m_end:
            metadata.update_dataset_metadata({"unit": f"{m_start}={base}"})
        else:
            metadata.update_dataset_metadata({"unit": f"{m_start}_{m_end}={base}"})

    metadata.add_transformation_step(
        {"rebase": {"start_date": m_start, "end_date": m_end, "base": base}}
    )

    return output, metadata
