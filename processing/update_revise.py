import pandas as pd


def upd_rev(df, prev_data, revise):

    previous_data = pd.read_csv(prev_data, sep=" ", index_col=0, header=[0, 1, 2, 3, 4, 5, 6, 7, 8])
    previous_data.index = pd.to_datetime(previous_data.index)
    non_revised = previous_data[:len(previous_data)-revise]
    revised = df[len(previous_data)-revise:]
    non_revised.columns = df.columns
    updated = non_revised.append(revised, sort=False)

    return updated
