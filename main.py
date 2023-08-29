from nicegui import ui
from nicegui.functions import refreshable
import pandas as pd
import database
import plotly.graph_objects as go
import os
import analysis_methods


@ui.refreshable
def display_dataframe():
    if df is not None:
        grid = ui.aggrid.from_pandas(df).props("style=height:400px")
    if df2 is not None:
        with ui.row():
            grid = ui.aggrid.from_pandas(df2).props(
                "style=height:300px style=width:85%"
            )
            with ui.card():
                ui.label("Result column:")
                ui.label(f"MEAN: {column_aggregates[0]}")
                ui.label(f"SUM: {column_aggregates[1]}")
                ui.label(f"MAX: {column_aggregates[2]}")
                ui.label(f"MIN: {column_aggregates[3]}")


@ui.refreshable
def display_plotly_chart():
    fig = go.Figure()
    if df is not None:
        print(df)
        first_column = df[df.columns[0]]
        for col in df.columns[1:]:
            fig.add_trace(
                go.Scatter(x=first_column, y=df[col], mode="lines+markers", name=col)
            )
        ui.plotly(fig).classes("w-full h-400")


def handle_upload(csv_file):
    global df
    df = database.csv_to_dataframe(csv_file.content)
    display_dataframe.refresh()
    calculate_columns.clear()
    handle_calculate_columns(calculate_columns, df)
    display_plotly_chart.refresh()
    select_file.close()
    
    


def handle_database_select(selected_database_name):
    global df
    df = database.database_to_dataframe(selected_database_name.value)
    display_dataframe.refresh()
    calculate_columns.clear()
    handle_calculate_columns(calculate_columns, df)
    display_plotly_chart.refresh()
    select_database.close()


def handle_merge_databases(database1_name, database2_name):
    global df
    df = analysis_methods.merge_databases(database1_name, database2_name)
    display_dataframe.refresh()
    join_dataframes.clear()
    join_dataframes.close()


@ui.refreshable
def handle_calculate_columns(dialog, df):
    with dialog:
        with ui.card():
            ui.label("Calculate columns")
            if df is not None and not df.empty:
                col1_select = ui.select(list(df.columns), label="First Column")
                col1_select.tailwind("w-40")
                col2_select = ui.select(list(df.columns), label="Second Column")
                col2_select.tailwind("w-40")
                opearation_select = ui.select(["+", "-", "*", "/"], label="Operation")
                opearation_select.tailwind("w-40")

                def apply_operation_to_columns():
                    global df2
                    global column_aggregates
                    selected_col1 = col1_select.value
                    selected_col2 = col2_select.value
                    selected_operation = opearation_select.value
                    analysis_methods.calculate_columns(
                        df, selected_col1, selected_col2, selected_operation
                    )
                    df2, column_aggregates = analysis_methods.calculate_columns(
                        df, selected_col1, selected_col2, selected_operation
                    )
                    display_dataframe.refresh()
                    calculate_columns.close()

                ui.button("Calculate", on_click=apply_operation_to_columns)
            else:
                ui.label("No data available.")


def hande_resample(resolution, aggregation):
    global df
    df = analysis_methods.resample_database(df, resolution, aggregation)
    display_dataframe.refresh()
    display_plotly_chart.refresh()
    resample.close()


def handle_save(save_as):
    database.dataframe_to_database(df, save_as)
    update_database_list()
    selected_database.update()
    save_to.close()
    selected_database.set_options(file_names)
    database1.set_options(file_names)
    database2.set_options(file_names)
    database1.update()
    database2.update()
    ui.update()


def handle_clear_data():
    global df
    global df2
    df = None
    df2 = None
    selected_database.set_value(None)
    selected_database.update()
    display_dataframe.refresh()    
    

def update_database_list():
    global file_names
    current_directory = os.path.dirname(os.path.abspath(__file__))
    databases_directory = os.path.join(current_directory, "databases")
    if not os.path.exists(databases_directory):
            os.makedirs(databases_directory)
    file_names = [
        file
        for file in os.listdir(databases_directory)
        if os.path.isfile(os.path.join(databases_directory, file))
    ]


df = None
df2 = None
column_aggregates = None
update_database_list()

with ui.header(elevated=True).style("background-color: #228b22").classes(
    "items-center"
):
    ui.label("Report").tailwind.text_align("center")

with ui.left_drawer():
    with ui.column():
        ui.label("Datasource")

        with ui.dialog() as select_file, ui.card():
            ui.upload(on_upload=handle_upload, auto_upload=True, label="CSV").tailwind("w-40")
            ui.button("Close", on_click=select_file.close)
        ui.button("CSV file", on_click=select_file.open)

        with ui.dialog() as select_database, ui.card():
            ui.label("Select database")
            selected_database = ui.select(file_names, on_change=handle_database_select)
            selected_database.tailwind("w-40")
            ui.button("Close", on_click=select_database.close)
        ui.button("Select database", on_click=select_database.open)

        with ui.dialog() as join_dataframes, ui.card():
            ui.label("Merge databases")
            database1 = ui.select(list(file_names), label="First database")
            database1.tailwind("w-40")
            database2 = ui.select(list(file_names), label="Second database")
            database2.tailwind("w-40")
            ui.button(
                "Merge",
                on_click=lambda: handle_merge_databases(
                    database1.value, database2.value
                ),
            )

        ui.button("Merge databases", on_click=join_dataframes.open)

        ui.label("Analysis methods")

        with ui.dialog() as calculate_columns:
            handle_calculate_columns(calculate_columns, df)
        ui.button("Calculate columns", on_click=calculate_columns.open)

        with ui.dialog() as resample, ui.card():
            ui.label("Select resolution and aggregation method")
            resolution = ui.select(
                ["Daily", "Weekly", "Monthly", "Quarterly", "Yearly"],
                label="Resolution",
            )
            resolution.tailwind("w-40")
            aggregation = ui.select(
                ["Mean", "Sum", "Count", "Max", "Min"], label="Aggregation"
            )
            aggregation.tailwind("w-40")
            ui.button(
                "Confirm",
                on_click=lambda: hande_resample(resolution.value, aggregation.value),
            )
        ui.button("Resample dataframe", on_click=resample.open)

        ui.label("Options")
        with ui.dialog() as save_to, ui.card():
            ui.label("Save as: ")
            save_as = ui.input(label="Name").on(
                "keydown.enter", lambda: handle_save(save_as.value)
            )
            ui.button("Save").on("click", lambda: handle_save(save_as.value))
        ui.button("Save", on_click=save_to.open)
        ui.button("Clear data", on_click=handle_clear_data)

with ui.tabs().classes("w-full") as tabs:
    one = ui.tab("Dataframe")
    two = ui.tab("Plotly chart")
with ui.tab_panels(tabs, value=one).classes("w-full"):
    with ui.tab_panel(one):
        ui.label("Dataframe")
        display_dataframe()
    with ui.tab_panel(two):
        ui.label("Plotly chart")
        display_plotly_chart()


ui.run()
