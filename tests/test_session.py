import logging
from os import listdir, remove, rmdir, path
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, inspect

from econuy.core import Pipeline
from econuy.session import Session
from econuy.utils import sqlutil, datasets, ops


CUR_DIR = path.abspath(path.dirname(__file__))
TEST_DIR = path.join(CUR_DIR, "test-data")
TEST_CON = create_engine("sqlite://")
sqlutil.insert_csvs(con=TEST_CON, directory=TEST_DIR)


def remove_temporary_files_folders():
    avoid = (
        list(datasets.original().keys()) + list(datasets.custom().keys()) + ["commodity_weights"]
    )
    avoid_csv = [f"{x}.csv" for x in avoid]

    for f in listdir(TEST_DIR):
        if f not in avoid_csv:
            try:
                remove(path.join(TEST_DIR, f))
            except IsADirectoryError:
                rmdir(path.join(TEST_DIR, f))

    for table in inspect(TEST_CON).get_table_names():
        if table not in avoid:
            TEST_CON.engine.execute(f'DROP TABLE IF EXISTS "{table}"')

    return


def test_wrong_dataset():
    p = Pipeline(location=TEST_DIR)
    session = Session.from_pipeline(p)
    for method in [session.get, session.get_bulk]:
        with pytest.raises(ValueError):
            method(names="wrong")


def test_save_no_location():
    s = Session()
    with pytest.raises(ValueError):
        s.save()


@pytest.mark.parametrize(
    "names,fmt,files",
    [
        (["cpi"], "xlsx", ["cpi.xlsx"]),
        (["cpi", "nxr_monthly"], "csv", ["cpi.csv", "nxr_monthly.csv"]),
    ],
)
def test_save(names, fmt, files, tmpdir):
    s = Session(tmpdir, always_save=False, save_fmt=fmt, read_fmt=fmt)
    s.get(names)
    s.save()
    for f in files:
        file_path = Path(tmpdir, f)
        assert path.isfile(file_path)
        assert not ops._read(file_path, file_fmt=fmt).empty


def test_save_different_formats(tmpdir):
    s = Session(
        tmpdir,
        read_fmt="xlsx",
        read_header="separate",
        save_fmt="xlsx",
        save_header="separate",
        download=False,
    )
    s.get("cpi")  # read: xlsx, separate; save: xlsx, separate
    first = s.datasets["cpi"].copy()

    s.save_fmt = "csv"
    s.save_header = "included"
    s.get("cpi")  # read: xlsx, separate; save: csv, included
    second = s.datasets["cpi"].copy()

    s.read_fmt = "csv"
    s.read_header = "included"
    s.save_header = None
    s.get("cpi")  # read: csv, included; save: csv, None
    third = s.datasets["cpi"].copy()

    s.read_header = None
    s.get("cpi")  # read: csv, None; save: csv, None
    fourth = s.datasets["cpi"].copy()

    fifth = third.copy()
    fifth.columns = fifth.columns.get_level_values(0)
    fifth.rename_axis(None, axis=1, inplace=True)

    assert all(v < 1.01 and v > 0.99 for v in (second / first).values)
    assert (second.columns == first.columns)[0] and (second.index == first.index)[0]
    assert all(v < 1.01 and v > 0.99 for v in (third / second).values)
    assert (third.columns == second.columns)[0] and (third.index == second.index)[0]
    assert all(v < 1.01 and v > 0.99 for v in (fifth / fourth).values)
    assert (fifth.columns == fourth.columns)[0] and (fifth.index == fourth.index)[0]


@pytest.mark.parametrize("log_level,file", [("test", "test.log"), (2, "info.log")])
def test_logging_file(caplog, log_level, file, tmpdir):
    remove_temporary_files_folders()
    caplog.clear()
    Session(location=tmpdir, log=log_level)
    test_path = path.join(tmpdir, file)
    assert path.isfile(test_path)
    logging.shutdown()
    caplog.clear()
    remove_temporary_files_folders()


def test_logging_error(caplog):
    with pytest.raises(ValueError):
        Session(location=TEST_DIR, log=5)
    caplog.clear()


@pytest.mark.parametrize("freq_resample,freq_result", [("A-DEC", "A-DEC"), ("M", "Q-DEC")])
def test_concat(freq_resample, freq_result):
    s = Session(location=TEST_CON, download=False)
    s.get(["natacc_ind_con_nsa", "public_debt_gps"])
    s.resample(select="natacc_ind_con_nsa", rule=freq_resample)
    s.concat(select="all", concat_name="test")
    df = s.datasets["concat_test"]
    combined_cols = (
        s.datasets["natacc_ind_con_nsa"]
        .columns.append(s.datasets["public_debt_gps"].columns)
        .get_level_values(0)
    )
    assert pd.infer_freq(df.index) == freq_result
    assert all(x in df.columns.get_level_values(0) for x in combined_cols)
