import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json


# ---------------------------------------------------------
# BigQuery Client
# ---------------------------------------------------------
@st.cache_resource
def get_dynamic_client(user_json: str):
    try:
        key_dict = json.loads(user_json)
        credentials = service_account.Credentials.from_service_account_info(key_dict)
        return bigquery.Client(credentials=credentials, project=credentials.project_id)
    except Exception as e:
        st.error(f"Invalid credentials: {e}")
        return None




# ---------------------------------------------------------
# Run Query (Graceful Error Handling)
# ---------------------------------------------------------
@st.cache_data(show_spinner=False)
def run_query(query: str):
    try:
        client = st.session_state.client
        rows = client.query_and_wait(query)
        return rows.to_dataframe(), None
    except Exception as e:
        safe_bigquery_error(e, context="Running SQL query")
        return None, str(e)


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def get_all_datasets():
    public_project = "bigquery-public-data"
    if st.session_state.client:
        client = st.session_state.client
        datasets = list(client.list_datasets(project=public_project))
        return [d.dataset_id for d in datasets]


def get_schema(dataset: str):
    query = f"""
        SELECT table_name
        FROM `bigquery-public-data.{dataset}.INFORMATION_SCHEMA.TABLES`
    """

    df, error = run_query(query)

    if error:
        st.session_state.query_error = error
        safe_bigquery_error(error, context="Loading dataset schema")
        st.error("Query failed. Please check your SQL.")
        return pd.DataFrame({"table_id": []})

    df = df.rename(columns={"table_name": "table_id"})
    return df

def user_key_handler(user_key_json):
    if user_key_json:
        st.session_state["user_key_json"] = user_key_json
       
        client = get_dynamic_client(st.session_state.user_key_json)

        if client:
            st.success("Key saved successfully")
            st.session_state.client = client
            st.session_state.update({"user_key_json": None})
        else:
            st.error("Invalid credentials. Please try again.")
        return True
    else:
        st.error("No key provided. Please paste your BigQuery key.")
        return False




# ---------------------------------------------------------
# Schema Change Detector (ONLY for SQL query results)
# ---------------------------------------------------------
def detect_schema_change(df):
    if df is None or not hasattr(df, "columns"):
        return False

    cols = tuple(df.columns.tolist())

    if "last_schema" not in st.session_state:
        st.session_state.last_schema = cols
        return True

    if st.session_state.last_schema != cols:
        st.session_state.last_schema = cols
        return True

    return False


# ---------------------------------------------------------
# Safe Error Message
# ---------------------------------------------------------
def safe_bigquery_error(error: Exception, context: str = ""):
    st.error(
        f"""
        Something went wrong while processing your request.

        **Context:** {context}

        This may be due to:
        - Temporary connection issues  
        - Missing or invalid credentials  
        - Insufficient permissions  
        - An unexpected BigQuery response  

        Please try again or contact the app administrator if the issue persists.
        """
    )


# ---------------------------------------------------------
# Plotting (Scatter, Line, Bar)
# ---------------------------------------------------------
def make_scatter_chart(df, x, y, legend_field, x_type, y_type):
    import altair as alt
    return (
        alt.Chart(df)
        .mark_point(size=80)
        .encode(
            x=alt.X(f"{x}:{x_type}", title=x),
            y=alt.Y(f"{y}:{y_type}", title=y),
            color=(
                alt.Color(legend_field, title="Legend")
                if legend_field else alt.value("steelblue")
            ),
            tooltip=[x, y],
        )
    )


def make_line_chart(df, x, y, legend_field, x_type, y_type):
    import altair as alt
    return (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{x}:{x_type}", title=x),
            y=alt.Y(f"{y}:{y_type}", title=y),
            color=(
                alt.Color(legend_field, title="Legend")
                if legend_field else alt.value("steelblue")
            ),
            tooltip=[x, y],
        )
    )


def make_bar_chart(df, x, y, legend_field, x_type, y_type):
    import altair as alt
    return (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(f"{x}:{x_type}", title=x),
            y=alt.Y(f"{y}:{y_type}", title=y),
            color=(
                alt.Color(legend_field, title="Legend")
                if legend_field else alt.value("steelblue")
            ),
            tooltip=[x, y],
        )
    )

def plotting_altair(df: pd.DataFrame, x: str, y: str, chart_type: str):
    import altair as alt

    if df is None or df.empty:
        st.warning("No data available to plot. Please run a valid SQL query.")
        return

    if x not in df.columns or y not in df.columns:
        st.warning(f"Selected fields are not valid. Columns: {df.columns.tolist()}")
        return

    df = df.copy()

    # Convert numeric-looking strings
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = pd.to_numeric(df[col], errors="ignore")

    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    categorical_cols = df.select_dtypes(exclude=["number"]).columns.tolist()

    x_type = "Q" if x in numeric_cols else "N"
    y_type = "Q" if y in numeric_cols else "N"

    legend_field = None
    if x in categorical_cols:
        legend_field = x
    elif y in categorical_cols:
        legend_field = y

    if legend_field and legend_field not in df.columns:
        legend_field = None

    # --- Choose chart ---
    if chart_type == "Scatter":
        chart = make_scatter_chart(df, x, y, legend_field, x_type, y_type)
    elif chart_type == "Line":
        chart = make_line_chart(df, x, y, legend_field, x_type, y_type)
    elif chart_type == "Bar":
        chart = make_bar_chart(df, x, y, legend_field, x_type, y_type)
    else:
        chart = make_scatter_chart(df, x, y, legend_field, x_type, y_type)

    chart = chart.properties(
        width="container",
        height=600,
        title=f"{chart_type} Chart"
    ).interactive()

    st.altair_chart(chart, use_container_width=True, width="stretch", height="content")


# ---------------------------------------------------------
# SQL Submit Handler
# ---------------------------------------------------------
def submit_handler_main(selected_dataset):
    query = st.session_state.main_query_text

    if not query or not query.strip():
        st.session_state.initial_df = None
        st.session_state.query_error = "Please enter a SQL query."
        return

    df, error = run_query(query)

    if error or df is None:
        st.session_state.initial_df = None
        st.session_state.query_error = error
        st.error("Query failed. Please check your SQL.")
        return

    # Store result
    st.session_state.initial_df = df

    # Detect schema change ONLY on SQL results
    if detect_schema_change(df):
        st.session_state.chart_x = None
        st.session_state.chart_y = None
        st.session_state.chart_type_selected = None
        st.session_state.plot_ready = False


# ---------------------------------------------------------
# Sidebar Chart Builder
# ---------------------------------------------------------
def build_sidebar_chart_builder():
    st.sidebar.title("Chart Builder")

    user_key_json = st.sidebar.text_area(
        "BigQuery key (JSON):",
        height=150,
        width="stretch",
        key="user_key_json"
        )
    
    user_key_submit = st.sidebar.button(
        "Save Key",
        on_click=lambda: user_key_handler(user_key_json),
        key="save_key_btn"
    )
 
    df = st.session_state.initial_df

    if df is None or df.empty:
        st.sidebar.info("Run a SQL query to enable charting")
        return

    all_cols = list(df.columns)

    x_field = st.sidebar.selectbox(
        "X-axis",
        all_cols,
        key="chart_x"
    )

    y_field = st.sidebar.selectbox(
        "Y-axis",
        all_cols,
        key="chart_y"
    )

    chart_type = st.sidebar.radio(
        "Chart Type",
        ["Scatter", "Line", "Bar"],
        key="chart_type_selected"
    )

    st.sidebar.button(
        "Plot",
        on_click=lambda: st.session_state.update({"plot_ready": True}),
        key="chart_builder_plot_btn"
    )

   


# ---------------------------------------------------------
# Plot Renderer
# ---------------------------------------------------------
def render_plot_if_ready():
    if not st.session_state.get("plot_ready"):
        return

    df = st.session_state.initial_df
    x = st.session_state.chart_x
    y = st.session_state.chart_y
    chart_type = st.session_state.chart_type_selected

    plotting_altair(df, x, y, chart_type)


# ---------------------------------------------------------
# Main View
# ---------------------------------------------------------
def build_main_view():
    st.title("BigQuery Explorer")

    datasets = get_all_datasets()
    selected_dataset = st.selectbox(
        "Select Dataset",
        datasets,
        key="main_dataset_select"
    )

    if selected_dataset != st.session_state.get("selected_dataset"):
        st.session_state.selected_dataset = selected_dataset
        st.session_state.schema = get_schema(selected_dataset)
        st.session_state.initial_df = None
        st.session_state.query_error = None
        st.session_state.plot_ready = False

    if st.session_state.schema is not None:
        st.write(f"ID: bigquery-public-data.{selected_dataset}")
        st.write("Tables:")
        st.dataframe(st.session_state.schema)

    st.text_area(
        "Enter SQL Query",
        height=150,
        key="main_query_text"
    )

    st.button(
        "Submit Query",
        on_click=submit_handler_main,
        args=(selected_dataset,),
        key="submit_main"
    )

    if st.session_state.initial_df is not None:
        st.write("Query Result:")
        st.dataframe(st.session_state.initial_df)


# ---------------------------------------------------------
# App Layout
# ---------------------------------------------------------
def init_state():
    defaults = {
        "schema": None,
        "selected_dataset": None,
        "initial_df": None,
        "query_error": None,
        "plot_ready": False,
        "chart_x": None,
        "chart_y": None,
        "chart_type_selected": None,
        "user_key_json": None,
        "client": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def build_layout():
    build_main_view()
    build_sidebar_chart_builder()
    render_plot_if_ready()


# ---------------------------------------------------------
# Run App
# ---------------------------------------------------------
if __name__ == "__main__":
    init_state()
    build_layout()