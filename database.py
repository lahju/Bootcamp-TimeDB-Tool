from tinyflux import TinyFlux, Point
from datetime import datetime, timezone
import pandas as pd
import os
from dateutil.parser import parser


def database_to_dataframe(database_name):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    databases_directory = os.path.join(current_directory, "databases")
    database = TinyFlux(f"{databases_directory}/{database_name}")
    points = database.all()
    data = []
    for point in points:
        row = {
            "timestamp": str(point.time),
            **point.fields,
        }
        data.append(row)
    df = pd.DataFrame(data)
    return df


def csv_to_dataframe(csv_file):
    df = pd.read_csv(csv_file, delimiter=";", decimal=",", skipinitialspace=True, encoding="latin1")
    return df


def dataframe_to_database(df, database_name):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    databases_directory = os.path.join(current_directory, "databases")
    db = TinyFlux(f"{databases_directory}/{database_name}.db")
    points_list = []
    time_column_name = df.columns[0]
    for index, row in df.iterrows():
        try:
            dt = datetime.strptime(row[time_column_name], "%d.%m.%Y %H:%M")
            dt = dt.replace(tzinfo=timezone.utc)
        except ValueError:
            dt = datetime.strptime(row[time_column_name], "%Y-%m-%d %H:%M:%S%z")
            dt = dt.replace(tzinfo=timezone.utc)
        fields = row.drop(time_column_name).to_dict()
        point = Point(time=dt, fields=fields)
        points_list.append(point)
    db.insert_multiple(points_list)
