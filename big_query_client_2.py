import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


# ---------------------------------------------------------
# BigQuery Client
# ---------------------------------------------------------
@st.cache_resource
def get_bigquery_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["bigquery"]
    )
    return bigquery.Client(credentials=credentials, project=credentials.project_id)

client = get_bigquery_client()


# ---------------------------------------------------------
# Run Query (Graceful Error Handling)
# ---------------------------------------------------------
@st.cache_data(show_spinner=False)
def run_query(query: str):
    try:
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
        return pd.DataFrame({"table_id": []})

    df = df.rename(columns={"table_name": "table_id"})
    return df

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
def plotting_altair(df: pd.DataFrame, x: str, y: str, chart_type: str):
    import altair as alt

    df = df.copy()

    # Auto-convert numeric-looking strings to numbers
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = pd.to_numeric(df[col], errors="ignore")

    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    categorical_cols = df.select_dtypes(exclude=["number"]).columns.tolist()

    x_type = "Q" if x in numeric_cols else "N"
    y_type = "Q" if y in numeric_cols else "N"

    # Legend logic
    legend_field = None
    if x in categorical_cols:
        legend_field = x
    elif y in categorical_cols:
        legend_field = y

    # Chart type selection
    if chart_type == "Scatter":
        chart = alt.Chart(df).mark_point(size=80)
    elif chart_type == "Line":
        chart = alt.Chart(df).mark_line(point=True)
    elif chart_type == "Bar":
        chart = alt.Chart(df).mark_bar()
    else:
        chart = alt.Chart(df).mark_point(size=80)

    chart = chart.encode(
        x=alt.X(f"{x}:{x_type}", title=x),
        y=alt.Y(f"{y}:{y_type}", title=y),
        color=alt.Color(legend_field, title="Legend") if legend_field else alt.value("steelblue"),
        tooltip=[x, y],
    ).properties(
        width="container",
        height=400,
        title=f"{chart_type} Chart"
    ).interactive()

    st.altair_chart(chart, use_container_width=True)


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

    if error:
        st.session_state.initial_df = None
        st.session_state.query_error = error
        return

    st.session_state.initial_df = df
    st.session_state.query_error = None


# ---------------------------------------------------------
# Chart Builder Submit Handler
# ---------------------------------------------------------
def submit_handler_chart_builder(x_field, y_field, chart_type):
    df = st.session_state.initial_df

    if df is None or df.empty:
        st.warning("Run a SQL query first")
        return

    plotting_altair(df, x_field, y_field, chart_type)


# ---------------------------------------------------------
# Sidebar: Chart Builder (Ideas 2 + 3 Combined)
# ---------------------------------------------------------
def build_sidebar_chart_builder():
    st.sidebar.title("Chart Builder")

    df = st.session_state.initial_df

    if df is None or df.empty:
        st.sidebar.info("Run a SQL query to enable charting")
        return

    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    all_cols = list(df.columns)

    default_x = numeric_cols[0] if numeric_cols else all_cols[0]
    default_y = numeric_cols[1] if len(numeric_cols) > 1 else all_cols[0]

    x_field = st.sidebar.selectbox(
        "X-axis",
        all_cols,
        index=all_cols.index(default_x),
        key="chart_x_field"
    )

    y_field = st.sidebar.selectbox(
        "Y-axis",
        all_cols,
        index=all_cols.index(default_y),
        key="chart_y_field"
    )

    chart_type = st.sidebar.radio(
        "Chart Type",
        ["Scatter", "Line", "Bar"],
        key="chart_type"
    )

    st.sidebar.button(
        "Plot",
        on_click=lambda: st.session_state.update({
            "plot_ready": True,
            "chart_x": x_field,
            "chart_y": y_field,
            "chart_type_selected": chart_type
        }),
        key="chart_builder_plot_btn"
    )

def render_plot_if_ready():
    if not st.session_state.get("plot_ready"):
        return

    df = st.session_state.initial_df
    x = st.session_state.chart_x
    y = st.session_state.chart_y
    chart_type = st.session_state.chart_type

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

    # Load schema only when dataset changes
    if selected_dataset != st.session_state.get("selected_dataset"):
        st.session_state.selected_dataset = selected_dataset
        st.session_state.schema = get_schema(selected_dataset)
        st.session_state.initial_df = None
        st.session_state.query_error = None

    # Show dataset tables
    if st.session_state.schema is not None:
        st.write(f"ID: bigquery-public-data.{selected_dataset}")
        st.write("Tables:")
        st.dataframe(st.session_state.schema)

    # SQL text area
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

    # Display SQL error
    if st.session_state.get("query_error"):
        st.error(st.session_state.query_error)

    # Display query output
    if st.session_state.initial_df is not None:
        st.write("Query Result:")
        st.dataframe(st.session_state.initial_df)

    return st.session_state.schema, selected_dataset

    


# ---------------------------------------------------------
# App Layout
# ---------------------------------------------------------
def init_state():
    defaults = {
        "schema": None,
        "selected_dataset": None,
        "initial_df": None,
        "query_error": None,
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