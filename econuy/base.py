from copy import deepcopy
from os import PathLike, makedirs, path
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
from sqlalchemy.engine.base import Connection, Engine

from econuy import transform
from econuy.utils import sqlutil

METADATA_NAMES = ["Indicador", "Área", "Frecuencia", "Moneda",
                  "Inf. adj.", "Unidad", "Seas. Adj.", "Tipo",
                  "Acum. períodos"]


class EconuyMetadata(object):
    def __init__(self, indicator: Union[List[str], str] = None,
                 area: Union[List[str], str] = None,
                 freq: Union[List[str], str] = None,
                 currency: Union[List[str], str] = None,
                 price_adj: Union[List[str], str] = None,
                 unit: Union[List[str], str] = None,
                 seas_adj: Union[List[str], str] = None,
                 typevar: Union[List[str], str] = None,
                 cum: Union[List[int], int] = None,
                 nindicators: int = 1):
        metas = []
        for meta in [indicator, area, freq, currency, price_adj,
                     unit, seas_adj, typevar, cum]:
            if isinstance(meta, (str, int)) or meta is None:
                metas.append([meta] * nindicators)
            elif isinstance(meta, List):
                if len(meta) == 1:
                    metas.append(meta * nindicators)
                elif len(meta) == nindicators:
                    metas.append(meta)
                else:
                    raise AttributeError("If multiple values are provided for "
                                         "a metadata category there should be "
                                         "as many as 'nindicators'.")
            else:
                raise AttributeError("Invalid parameter set.")
        self.indicator = metas[0]
        self.area = metas[1]
        self.freq = metas[2]
        self.currency = metas[3]
        self.price_adj = metas[4]
        self.unit = metas[5]
        self.seas_adj = metas[6]
        self.typevar = metas[7]
        self.cum = metas[8]
        self.nindicators = nindicators

    def __repr__(self):
        return repr(self.__dict__)

    @property
    def all_equal(self):
        frame = self.to_frame().drop(["Indicador"], axis=1)
        if all(frame.iloc[i].equals(frame.iloc[0])
               for i in range(self.nindicators)):
            return True
        else:
            return False

    def to_tuples(self):
        arrays = [x for x in self.to_dict().values()]
        return list(zip(*arrays))

    def to_frame(self):
        frame = pd.DataFrame(self.to_dict())
        frame.columns = METADATA_NAMES
        return frame

    def to_dict(self):
        meta_dict = {k: v for k, v in self.__dict__.items()
                     if k not in ["nindicators"]}
        return meta_dict

    def modify(self,
               indicator: Union[List[str], str] = None,
               area: Union[List[str], str] = None,
               freq: Union[List[str], str] = None,
               currency: Union[List[str], str] = None,
               price_adj: Union[List[str], str] = None,
               unit: Union[List[str], str] = None,
               seas_adj: Union[List[str], str] = None,
               typevar: Union[List[str], str] = None,
               cum: Union[List[int], int] = None,
               inplace: bool = False):
        new_metadata = deepcopy(self.__dict__)
        for k, v in zip(new_metadata.keys(), [indicator, area, freq, currency,
                                              price_adj, unit, seas_adj,
                                              typevar, cum]):
            if v is None:
                continue
            new_metadata.update({k: v})
        if inplace is True:
            self.metadata = EconuyMetadata(**new_metadata)
            return
        else:
            return EconuyMetadata(**new_metadata)


class EconuyDF(pd.DataFrame):
    _metadata = ["dataset_name", "metadata", "custom_metadata", "location"]
    dataset_name = None
    metadata = None
    custom_metadata = None
    location = None

    def __init__(self,
                 *args,
                 dataset_name: Optional[str] = None,
                 custom_metadata: Optional[Dict] = None,
                 location: Union[str, PathLike,
                                 Connection, Engine, None] = "econuy-data",
                 **kwargs):
        super(EconuyDF, self).__init__(*args, copy=True, **kwargs)
        self.dataset_name = dataset_name
        self.location = location
        self._nindicators = len(self._indicator_axis)
        if self.metadata is None:
            if custom_metadata is None:
                self.metadata = self.metadata_from_axis()
            else:
                custom_metadata["indicator"] = self.columns.to_list()
                self.custom_metadata = custom_metadata
                self.metadata = EconuyMetadata(**custom_metadata,
                                               nindicators=self._nindicators)
        self._validate()
        self._drop_extra_metadata(inplace=True)

    @property
    def _constructor(self):
        return EconuyDF

    @property
    def _constructor_sliced(self):
        return pd.Series

    @property
    def base_df(self):
        return pd.DataFrame(self)

    @property
    def _indicator_axis(self):
        if isinstance(self.index, pd.DatetimeIndex):
            return self.columns
        else:
            return self.index

    @property
    def _indicator_axis_name(self):
        if isinstance(self.index, pd.DatetimeIndex):
            return "columns"
        else:
            return "index"

    def _validate(self):
        if self.equals(pd.DataFrame()):
            return
        if not any(isinstance(axis, pd.DatetimeIndex)
                   for axis in [self.columns, self.index]):
            raise AttributeError("At least one of the axis must be a "
                                 "DatetimeIndex.")
        if (self.metadata is None
                and not any(isinstance(axis, pd.MultiIndex)
                            for axis in [self.columns, self.index])):
            raise AttributeError("Custom metadatata must be provided if none "
                                 "of the axis are a MultiIndex.")

    def _drop_extra_metadata(self, inplace=False):
        if inplace is True:
            if self._indicator_axis_name == "columns":
                self.columns = self.columns.get_level_values(0)
            else:
                self.index = self.index.get_level_values(0)
            return
        else:
            output = self.copy()
            if output._indicator_axis_name == "columns":
                output.columns = output.columns.get_level_values(0)
            else:
                output.index = output.index.get_level_values(0)
            return output

    def metadata_from_axis(self):
        metadata_vars = ["indicator", "area", "freq", "currency", "price_adj",
                         "unit", "seas_adj", "typevar", "cum"]
        levels = self._indicator_axis.nlevels
        if levels != 1:
            if not all(x in self._indicator_axis.names
                       for x in METADATA_NAMES[9 - levels:]):
                raise AttributeError("Missing axis labels.")
        parsed_columns = {k: self._indicator_axis.get_level_values(v).to_list()
                          for k, v in zip(metadata_vars[9 - levels:],
                                          range(levels))}
        return EconuyMetadata(**parsed_columns, nindicators=self._nindicators)

    def axis_from_metadata(self, inplace: bool = False):
        multiindex = pd.MultiIndex.from_tuples(self.metadata.to_tuples(),
                                               names=METADATA_NAMES)
        if inplace is True:
            if self._indicator_axis_name == "columns":
                self.columns = multiindex
            else:
                self.index = multiindex
            return
        else:
            output = self.copy()
            if output._indicator_axis_name == "columns":
                output.columns = multiindex
            else:
                output.index = multiindex
            return output

    def modify_metadata(self, inplace: bool = False, 
                        set_axis: bool = False, **kwargs):
        if inplace is True:
            self.metadata = self.metadata.modify(inplace=False, **kwargs)
            if set_axis is True:
                self.axis_from_metadata(inplace=True)
            return
        else:
            output = self.copy()
            output.metadata = output.metadata.modify(inplace=False, **kwargs)
            if set_axis is True:
                output.axis_from_metadata(inplace=True)
            return output

    def save(self, method: str,
             location: Union[str, PathLike, Connection, Engine, None] = None,
             name: Optional[str] = None,
             metadata: str = "separate"):
        """Saves dataframe to CSV, Excel or SQL.

        Parameters
        ----------
        method : str
            Save method.
        location : Union[str, PathLike, Connection, Engine, None], optional
            Folder or SQLAlchemy database where to save the data, by default 
            None which looks uses the object's `location` attribute.
        name : Optional[str], optional
            Filename or SQL table name, by default "data"
        metadata : str, optional
            For the `excel` method, what to do with metadata, by default 
            "separate". `columns` saves it as a MultiIndex above the data. 
            `separate` saves it in a separate sheet. `remove` does not save the 
            metadata at all.

        Raises
        ------
        AttributeError
            If `method` is not one of the available options.
        AttributeError
            If `meatadata` is not one of the available options.
        AttributeError
            If `csv` or `excel` are chosen and `location` is not a string or 
            Pathlike object
        """
        if method not in ["csv", "excel", "sql"]:
            raise AttributeError("'method' must be one of 'csv', 'excel' "
                                 "or 'sql'.")
        if metadata not in ["separate", "columns", "remove"]:
            raise AttributeError("'metadata' must be one of 'separate', "
                                 "'columns', or 'remove'.")
        if location is None:
            if self.location is None:
                location = "econuy-data"
            else:
                location = self.location
        if name is None:
            if self.dataset_name is None:
                name = "data"
            else:
                name = self.dataset_name
        if method in ["csv", "excel"]:
            if isinstance(location, (Engine, Connection)):
                raise AttributeError("If CSV or Excel is chosen, location must "
                                     "be a string or a Pathlike object.")
            if not path.exists(location):
                makedirs(location)
            if method == "csv":
                location = Path(location, name).with_suffix(".csv")
                self.to_csv(location, sep=",", encoding="latin1")
            elif method == "excel":
                location = Path(location, name).with_suffix(".xlsx")
                if metadata == "columns":
                    output = self.axis_from_metadata(inplace=False)
                    output.to_excel(location, sheet_name="Data")
                elif metadata == "separate":
                    output = self._drop_extra_metadata(inplace=False)
                    metadata_frame = (self.metadata.to_frame()
                                      .set_index("Indicador").T)
                    with pd.ExcelWriter(location) as f:
                        output.to_excel(f, sheet_name="Data")
                        metadata_frame.to_excel(f, sheet_name="Metadata",
                                                header=True)
                else:
                    output = self._drop_extra_metadata(inplace=False)
                    output.to_excel(location, sheet_name="Data")
        elif method == "sql":
            if not isinstance(location, (Engine, Connection)):
                raise AttributeError("If SQL is chosen, location must be an "
                                     "Engine or Connection object.")
            sqlutil.df_to_sql(self, name=name, con=location,
                              index_label="index")
        return

    def e_resample(self, rule, operation="sum", interpolation="linear"):
        return transform.resample(self, rule=rule, operation=operation,
                                  interpolation=interpolation)

    def e_rolling(self, window, operation="sum"):
        return transform.rolling(self, window=window, operation=operation)
