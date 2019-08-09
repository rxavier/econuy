from processing import colnames


def base_index(df, start_date, end_date=None, base=100):

    if end_date is None:
        indexed = df.apply(lambda x: x / x[start_date] * base)
        colnames.set_colnames(indexed, index=start_date)

    else:
        indexed = df.apply(lambda x: x / x[start_date:end_date].mean() * base)
        colnames.set_colnames(indexed, index=f"{start_date}_{end_date}")

    return indexed
