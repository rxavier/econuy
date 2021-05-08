import logging
import shutil
from os import listdir, remove, path
from pathlib import Path
from typing import Tuple

import pandas as pd
import pytest
from sqlalchemy import create_engine, inspect

from econuy.session import Session
from econuy.utils import sqlutil

try:
    from tests.test_transform import dummy_df
except ImportError:
    from .test_transform import dummy_df

CUR_DIR = path.abspath(path.dirname(__file__))
TEST_DIR = path.join(CUR_DIR, "test-data")
TEST_CON = create_engine("sqlite://").connect()
sqlutil.insert_csvs(con=TEST_CON, directory=TEST_DIR)


def remove_clutter(avoid: Tuple[str] = ("reserves_changes.csv",
                                        "commodity_weights.csv",
                                        "nxr_daily.csv")):
    [remove(path.join(TEST_DIR, x)) for x in listdir(TEST_DIR)
     if x not in avoid]

    for table in inspect(TEST_CON).get_table_names():
        if table not in [f[:-4] for f in avoid]:
            TEST_CON.engine.execute(f'DROP TABLE IF EXISTS "{table}"')

    return


def test_wrong_dataset():
    remove_clutter()
    session = Session(location=TEST_DIR)
    for method in [session.get, session.get_custom, session.get_bulk]:
        with pytest.raises(ValueError):
            method(dataset="wrong")


def test_save():
    remove_clutter()
    data = dummy_df(freq="M")
    session = Session(location=TEST_DIR, dataset=data)
    session.save(name="test_save")
    assert path.isfile(Path(TEST_DIR) / "test_save.csv")
    remove_clutter()
    data = dummy_df(freq="M")
    session = Session(location=TEST_DIR, dataset={
        "data1": data, "data2": data})
    session.save(name="test_save")
    assert path.isfile(Path(TEST_DIR) / "test_save_data1.csv")
    assert path.isfile(Path(TEST_DIR) / "test_save_data2.csv")
    remove_clutter()
    session.location = "new_dir"
    session.save(name="test_save")
    assert path.isfile(Path(session.location) / "test_save_data1.csv")
    assert path.isfile(Path(session.location) / "test_save_data2.csv")
    session.dataset = data
    session.save(name="test_save")
    assert path.isfile(Path(session.location) / "test_save.csv")
    remove_clutter()
    shutil.rmtree(session.location)


def test_logging(caplog):
    remove_clutter()
    caplog.clear()
    s = Session(location=TEST_DIR, log="test")
    test_path = path.join(TEST_DIR, "test.log")
    assert path.isfile(test_path)
    logging.shutdown()
    caplog.clear()
    s = Session(location=TEST_DIR, log=2)
    info_path = path.join(TEST_DIR, "info.log")
    assert path.isfile(info_path)
    logging.shutdown()
    caplog.clear()
    with pytest.raises(ValueError):
        Session(location=TEST_DIR, log=5)
    caplog.clear()
    remove_clutter()
    Session(location=TEST_DIR, log=1)
    assert "Logging method: console" in caplog.text
    assert "Logging method: console and file" not in caplog.text
    assert not path.isfile(path.join(TEST_DIR, "info.log"))
    caplog.clear()
    Session(location=TEST_DIR, log=0)
    assert caplog.text == ""
    logging.shutdown()
