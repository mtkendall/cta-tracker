"""
CTA Transit Tracker — Streamlit Dashboard

Shows headway analysis (time between consecutive vehicle arrivals) for tracked
CTA train and bus routes, broken out by time of day and day of week.

Two data source modes:
  - Local (default): reads from data/cta.duckdb (set DB_PATH to override)
  - Parquet (cloud):  reads from exports/*.parquet committed to git
    Set DATA_SOURCE=parquet in Streamlit secrets to activate this mode.

Run with:
    streamlit run app/streamlit_app.py
"""

import os
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "data/cta.duckdb")
DATA_SOURCE = os.getenv("DATA_SOURCE", "duckdb")   # "duckdb" or "parquet"
EXPORTS_DIR = Path("exports")

MART_TABLES = ["on_time_by_route_hour", "headway_stats"]

st.set_page_config(
    page_title="CTA Tracker",
    page_icon="🚊",
    layout="wide",
)

st.title("CTA Transit Tracker")
st.caption("Headway analysis for nearby train and bus routes.")


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Returns a DuckDB connection.

    In 'duckdb' mode: connects to the local .duckdb file (read-only).
    In 'parquet' mode: creates an in-memory DB with views over exports/*.parquet,
      so all downstream SQL queries work identically.
    """
    if DATA_SOURCE == "parquet":
        conn = duckdb.connect()   # in-memory
        for table in MART_TABLES:
            parquet_path = EXPORTS_DIR / f"{table}.parquet"
            if parquet_path.exists():
                conn.execute(
                    f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{parquet_path}')"
                )
        return conn
    else:
        return duckdb.connect(DB_PATH, read_only=True)


def has_table(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    result = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return result[0] > 0


try:
    conn = get_connection()
except Exception as e:
    st.error(f"Could not open data source ({DATA_SOURCE}): {e}")
    st.stop()

# Check that headway data is available
if not has_table(conn, "headway_stats"):
    if DATA_SOURCE == "parquet":
        st.warning(
            "No exported data found in `exports/`. On your local machine, run:\n\n"
            "```bash\n"
            "python scripts/collect_data.py\n"
            "python scripts/run_dbt.py\n"
            "python scripts/export_for_replit.py\n"
            "git add exports/ && git push\n"
            "```"
        )
    else:
        st.warning(
            "No transformed data found. Run the following to get started:\n\n"
            "```bash\n"
            "python scripts/collect_data.py    # collect some data first\n"
            "python scripts/run_dbt.py         # build the mart tables\n"
            "```"
        )
    st.stop()

# ── Sidebar filters ──────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Filters")

    modes = conn.execute(
        "SELECT DISTINCT mode FROM headway_stats ORDER BY mode"
    ).df()["mode"].tolist()
    selected_mode = st.radio("Mode", modes, horizontal=True)

    ROUTE_LABELS = {"Brn": "Brown", "P": "Purple"}

    routes = conn.execute(
        "SELECT DISTINCT route FROM headway_stats WHERE mode = ? ORDER BY route",
        [selected_mode],
    ).df()["route"].tolist()
    route_labels = [ROUTE_LABELS.get(r, r) for r in routes]
    selected_route_label = st.selectbox("Route", route_labels, index=0)
    selected_route = routes[route_labels.index(selected_route_label)]

    destinations = conn.execute(
        "SELECT DISTINCT destination FROM headway_stats WHERE mode = ? AND route = ? ORDER BY destination",
        [selected_mode, selected_route],
    ).df()["destination"].tolist()
    selected_dest = st.selectbox("Destination", destinations, index=0)

    stop_rows = conn.execute(
        "SELECT DISTINCT stop_id, stop_name FROM headway_stats WHERE mode = ? AND route = ? AND destination = ? ORDER BY stop_name",
        [selected_mode, selected_route, selected_dest],
    ).df()
    stop_labels = stop_rows["stop_name"].tolist()
    stop_ids = stop_rows["stop_id"].tolist()
    selected_stop_label = st.selectbox("Stop", stop_labels, index=0)
    selected_stop_id = stop_ids[stop_labels.index(selected_stop_label)]

    time_window = st.selectbox(
        "Time window",
        ["All time", "Last 7 days", "Last 30 days"],
        index=0,
    )

    day_options = ["All days", "Weekdays", "Weekends"]
    day_filter = st.selectbox("Day filter", day_options)

# ── Build filter clauses ──────────────────────────────────────────────────────

if day_filter == "Weekdays":
    day_clause = "AND day_of_week BETWEEN 1 AND 5"
elif day_filter == "Weekends":
    day_clause = "AND day_of_week IN (0, 6)"
else:
    day_clause = ""

if time_window == "Last 7 days":
    date_clause = f"AND collected_date >= '{date.today() - timedelta(days=7)}'"
elif time_window == "Last 30 days":
    date_clause = f"AND collected_date >= '{date.today() - timedelta(days=30)}'"
else:
    date_clause = ""

base_filter = f"mode = ? AND route = ? AND stop_id = ? AND destination = ? {day_clause} {date_clause}"
base_params = [selected_mode, selected_route, selected_stop_id, selected_dest]

# ── Load headway data ─────────────────────────────────────────────────────────

heatmap_df: pd.DataFrame = conn.execute(f"""
    SELECT
        hour_of_day,
        day_name,
        day_of_week,
        sum(observation_count)                                              AS observation_count,
        round(
            sum(avg_headway_minutes * observation_count) / sum(observation_count), 1
        )                                                                   AS avg_headway_minutes,
        round(
            sum(p90_headway_minutes * observation_count) / sum(observation_count), 1
        )                                                                   AS p90_headway_minutes,
        max(max_headway_minutes)                                            AS max_headway_minutes
    FROM headway_stats
    WHERE {base_filter}
    GROUP BY hour_of_day, day_name, day_of_week
    ORDER BY day_of_week, hour_of_day
""", base_params).df()


# ── Summary metrics ───────────────────────────────────────────────────────────

stop_name = stop_rows.loc[stop_rows["stop_id"] == selected_stop_id, "stop_name"].iloc[0]
st.subheader(f"{selected_mode.title()} {selected_route_label} → {selected_dest} — {stop_name}")

total_obs = int(heatmap_df["observation_count"].sum()) if not heatmap_df.empty else 0

if total_obs > 0:
    overall_avg = round(
        (heatmap_df["avg_headway_minutes"] * heatmap_df["observation_count"]).sum()
        / heatmap_df["observation_count"].sum(),
        1,
    )
    overall_p90 = round(
        (heatmap_df["p90_headway_minutes"] * heatmap_df["observation_count"]).sum()
        / heatmap_df["observation_count"].sum(),
        1,
    )
    overall_max = int(heatmap_df["max_headway_minutes"].max())
else:
    overall_avg = overall_p90 = overall_max = None

col1, col2, col3, col4 = st.columns(4)
col1.metric("Avg headway", f"{overall_avg} min" if overall_avg is not None else "—")
col2.metric("P90 headway", f"{overall_p90} min" if overall_p90 is not None else "—")
col3.metric("Max headway", f"{overall_max} min" if overall_max is not None else "—")
col4.metric("Observations", f"{total_obs:,}")

st.divider()

# ── Headway heatmap ───────────────────────────────────────────────────────────

metric_col, _ = st.columns([1, 3])
with metric_col:
    metric_choice = st.radio(
        "Heatmap metric",
        ["Avg", "P90", "Max"],
        horizontal=True,
    )

metric_field = {"Avg": "avg_headway_minutes", "P90": "p90_headway_minutes", "Max": "max_headway_minutes"}[metric_choice]
st.subheader(f"{metric_choice} headway by hour and day (minutes)")

if heatmap_df.empty:
    st.info("No headway data yet for this stop. Collect more data and re-run dbt.")
else:
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    heatmap_pivot = (
        heatmap_df
        .pivot(index="day_name", columns="hour_of_day", values=metric_field)
        .reindex([d for d in day_order if d in heatmap_df["day_name"].values])
        .reindex(columns=range(24))
    )

    data_max = heatmap_df[metric_field].max()
    zmax = max(round(data_max / 10) * 10, 10)   # round up to nearest 10, minimum 10

    fig = px.imshow(
        heatmap_pivot,
        color_continuous_scale="RdYlGn_r",   # reversed: green = short gap = frequent service
        zmin=0,
        zmax=zmax,
        labels={"x": "Hour of day", "y": "", "color": f"{metric_choice} headway (min)"},
        aspect="auto",
        text_auto=True,
    )
    fig.update_layout(
        coloraxis_colorbar_title="Min",
        xaxis=dict(
            tickmode="array",
            tickvals=list(range(24)),
            ticktext=[f"{h}:00" for h in range(24)],
        ),
        margin=dict(l=0, r=0, t=20, b=0),
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "Green = frequent service (short gaps), Red = infrequent service (long gaps). "
        f"Based on {total_obs:,} headway observations."
    )

st.divider()

# ── Headway trend line chart ──────────────────────────────────────────────────

st.subheader("Daily avg headway over time")

DAY_LABELS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
DAY_NUMBERS = {label: i for i, label in enumerate(DAY_LABELS)}

trend_col1, trend_col2, trend_col3 = st.columns(3)
with trend_col1:
    selected_days = st.multiselect(
        "Days of week",
        options=DAY_LABELS,
        default=DAY_LABELS,
    )
with trend_col2:
    hour_range = st.slider("Hour of day", min_value=0, max_value=23, value=(0, 23))
with trend_col3:
    trend_metric = st.radio("Metric", ["Avg", "P90", "Max"], horizontal=True)

trend_metric_field = {
    "Avg": "avg_headway_minutes",
    "P90": "p90_headway_minutes",
    "Max": "max_headway_minutes",
}[trend_metric]

selected_day_nums = [DAY_NUMBERS[d] for d in selected_days] if selected_days else list(range(7))
day_placeholders = ", ".join("?" * len(selected_day_nums))
trend_filter = (
    f"{base_filter}"
    f" AND day_of_week IN ({day_placeholders})"
    f" AND hour_of_day BETWEEN ? AND ?"
)
trend_params = base_params + selected_day_nums + list(hour_range)

trend_df: pd.DataFrame = conn.execute(f"""
    SELECT
        collected_date,
        sum(observation_count)                                              AS observation_count,
        round(
            sum(avg_headway_minutes * observation_count) / sum(observation_count), 1
        )                                                                   AS avg_headway_minutes,
        round(
            sum(p90_headway_minutes * observation_count) / sum(observation_count), 1
        )                                                                   AS p90_headway_minutes,
        max(max_headway_minutes)                                            AS max_headway_minutes
    FROM headway_stats
    WHERE {trend_filter}
    GROUP BY collected_date
    ORDER BY collected_date
""", trend_params).df()

if trend_df.empty or len(trend_df) < 2:
    st.info("Collect at least 2 days of data to see the trend chart.")
else:
    fig2 = px.line(
        trend_df,
        x="collected_date",
        y=trend_metric_field,
        markers=True,
        labels={"collected_date": "Date", trend_metric_field: f"{trend_metric} headway (min)"},
    )
    fig2.update_layout(yaxis_range=[0, None], margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(fig2, use_container_width=True)

