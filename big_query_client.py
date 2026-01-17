from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st
import db_dtypes as dbt
import pandas as pd


                            ################### ------------------------------------------------- ###########################
# build layout
# -- text area
# -- submit button
# -- sidebar
# after hit submit
# display datafrmae previe of dataset schema and checkboxed for schema tables
# user selects checkboxes and hits submit (submit button for sidebar)
# display dataframe preview of selected tables


import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


@st.cache_resource
def get_bigquery_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["bigquery"]
    )
    return bigquery.Client(credentials=credentials, project=credentials.project_id)
client = get_bigquery_client()


@st.cache_data
def run_query(query: str):
    rows = client.query_and_wait(query)
    return rows.to_dataframe()


def query_handler(query: str):
    if not query.strip():
        st.error("Please enter a query")
        return None
    df = run_query(query)
    st.success("Query executed successfully")
    return df


@st.cache_resource
def get_all_datsets():
    # The client handles authentication and project selection
    # It uses Application Default Credentials by default.

    print("Datasets in the bigquery-public-data project:")
    # Specify the project ID for public data
    public_data_project_id = "bigquery-public-data"
    datasets = list(client.list_datasets(project=public_data_project_id))

    if datasets:
        return [dataset.dataset_id for dataset in datasets]
    else:
        print("No datasets found.")
        return []


def get_schema(dataset: str, limit=None): 
    preview_query = None
    if limit:
        preview_query = f"SELECT * FROM `bigquery-public-data.{dataset}.__TABLES__` LIMIT {limit}"
    else:
        preview_query = f"SELECT * FROM `bigquery-public-data.{dataset}.__TABLES__`"
    df = query_handler(preview_query)
    return df

def build_main_view():
    st.title("Big Query Client")
    datasets = get_all_datsets()

    if not datasets:
        st.write("No datasets found")
        return None, None

    selected_dataset = st.selectbox("Select a dataset", options=datasets)
    query = st.text_area("query", height=150)

    if selected_dataset != st.session_state.get("selected_dataset"):
        st.session_state.main_submitted = False

    st.button(
        "Submit",
        on_click=submit_handler_main,
        args=(selected_dataset,),
        key="submit_main"
    )

    return (
        st.session_state.get("schema"),
        st.session_state.get("selected_dataset")
    )

def build_sidebar(schema, selected_dataset):

    st.sidebar.title("Big Query Datasets")
    st.sidebar.write("Select tables:")

    if st.session_state.main_submitted:
        boxes = list(schema.table_id)
        selected_tables = [
            t for t in boxes
            if st.sidebar.checkbox(t, key=f"chk_{t}")
        ]
    else:
        selected_tables = []

    st.sidebar.button(
        "Submit",
        on_click=submit_handler_sidebar,
        args=(selected_dataset, selected_tables, schema),
        disabled=not st.session_state.main_submitted,
        key="submit_sidebar"
    )

    return selected_tables

def build_layout():
    schema, dataset = build_main_view()

    # Sidebar updates automatically based on state
    tables = build_sidebar(schema, dataset)


def safe_extract(df, cols):
    return df[[c for c in cols if c in df.columns]]

def plotting_demo_st(df: pd.DataFrame = None):
    default_data = {
        "num_legs": [2, 4, 8, 0],
        "num_wings": [2, 0, 0, 0],
        "num_specimen_seen": [10, 2, 1, 8],
    }
    df_default = pd.DataFrame(default_data, index=["falcon", "dog", "spider", "fish"])

    if df is None or df.empty:
        df = df_default

    # Only numeric columns for Streamlit charts
    numeric_df = df.select_dtypes(include=["number"])

    if numeric_df.empty:
        st.warning("No numeric columns available for plotting")
        st.dataframe(df)
        return

    st.dataframe(numeric_df)
    st.line_chart(numeric_df)



def plotting_demo_alt(query: str):
    import altair as alt
    import streamlit as st

    df_alt = query_handler(query)
    df_alt = df_alt[["name", "total", "gender"]]
    default_data = {
        "num_legs": [2, 4, 8, 0],
        "num_wings": [2, 0, 0, 0],
        "num_specimen_seen": [10, 2, 1, 8],
    }
    df_default = pd.DataFrame(default_data, index=["falcon", "dog", "spider", "fish"])
    if df_alt is None:
        df_alt = df_default

    chart = (
        alt.Chart(df_alt)
        #.mark_line()
        .mark_point()
        .encode(
            x=alt.X("name:N", title="Name"),
            y=alt.Y("total:Q", title="Total"),
            color=alt.Color("gender:N", scale=alt.Scale(scheme="category10")),
            tooltip=["name", "total", "gender"]
        )
        .properties(
            width="container",
            height=400,
            title="My Custom Line Chart"
        )
    )
    chart = chart.interactive()
    st.altair_chart(chart, use_container_width=True)


def submit_handler_main(selected_dataset: str):
    st.session_state.main_submitted = True
    st.session_state.selected_dataset = selected_dataset
    st.session_state.schema = get_schema(selected_dataset)
    if selected_dataset:
        st.write(f"Selected: {selected_dataset}\n\nID: bigquery-public-data.{selected_dataset}")
        schema = get_schema(selected_dataset)
        st.dataframe(schema)
        
        return schema, selected_dataset

def submit_handler_sidebar(selected_dataset: str, tables: list, schema: pd.DataFrame):
    if not tables:
        print("boxes are not selected")
        return

    dataset_path = f"bigquery-public-data.{selected_dataset}"

    # Load the first selected table
    table = tables[0]
    full_table_path = f"{dataset_path}.{table}"

    df = run_query(f"SELECT * FROM `{full_table_path}` LIMIT 500")

    # If your checkboxes represent TABLES, not columns:
    df_small = df

    # If later your checkboxes represent COLUMNS, use:
    # df_small = safe_extract(df, tables)

    plotting_demo_st(df_small)

def init_state():
    if "main_submitted" not in st.session_state:
        st.session_state.main_submitted = False

    if "schema" not in st.session_state:
        st.session_state.schema = None

    if "selected_dataset" not in st.session_state:
        st.session_state.selected_dataset = None

if __name__ == "__main__":
    init_state()
    build_layout()