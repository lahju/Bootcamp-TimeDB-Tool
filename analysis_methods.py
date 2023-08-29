import pandas as pd
from tinyflux import TinyFlux, Point
import os
import database


def calculate_columns(df, col1, col2, operation):
    if operation == "+":
        result = df[col1] + df[col2]
    elif operation == "-":
        result = df[col1] - df[col2]
    elif operation == "*":
        result = df[col1] * df[col2]
    elif operation == "/":
        try:
            result = df[col1] / df[col2]
        except ZeroDivisionError:
            result = pd.Series([float('nan')]*len(df[col1]))
    data = {"Timestamp": df.iloc[:, 0], col1: df[col1], col2: df[col2], "Result": result}
    df2 = pd.DataFrame(data)
    df2 = df2.round(2)
    column_aggregates = [
    round(df2["Result"].mean(), 2),
    round(df2["Result"].sum(), 2),
    round(df2["Result"].max(), 2),
    round(df2["Result"].min(), 2)
]
    print(df2)

    return df2, column_aggregates


def merge_databases(database1_name, database2_name):
    database1 = database.database_to_dataframe(database1_name)
    database2 = database.database_to_dataframe(database2_name)
    col_name_db1 = database1.columns[0]
    col_name_db2 = database2.columns[0]
    merged_df = pd.merge(
        database1, database2, left_on=col_name_db1, right_on=col_name_db2, how="outer"
    )
    return merged_df


def resample_database(df, resolution, aggregation):
    first_column_name = df.columns[0]
    df[first_column_name] = pd.to_datetime(df[first_column_name])
    df.set_index(first_column_name, drop=False, inplace=True)
    if aggregation == "Mean":
        aggregation = df.resample(resolution[0]).mean()
    elif aggregation == "Sum":
        aggregation = df.resample(resolution[0]).sum()
    elif aggregation == "Count":
        aggregation = df.resample(resolution[0]).count()
    elif aggregation == "Max":
        aggregation = df.resample(resolution[0]).max()
    elif aggregation == "Min":
        aggregation = df.resample(resolution[0]).min()
    df = aggregation
    df = df.reset_index()
    df = df.round(2)
    df[first_column_name] = df[first_column_name].dt.strftime("%Y-%m-%dT%H:%M:%S")
    return df
