"""
CTA Transit Tracker — Streamlit Dashboard

Shows historical on-time performance for tracked CTA train and bus routes.

Run with:
    streamlit run app/streamlit_app.py
"""

import os

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "data/cta.duckdb")
ON_TIME_THRESHOLD = 1  # minutes; used for any threshold-based display

st.set_page_config(
    page_title="CTA Tracker",
    page_icon="🚊",
    layout="wide",
)

st.title("CTA Transit Tracker")
st.caption("Historical on-time performance for nearby train and bus routes.")


@st.cache_resource
def get_connection():
    """Shared read-only DuckDB connection (cached for the session)."""
    return duckdb.connect(DB_PATH, read_only=True)


def has_table(conn, table_name: str) -> bool:
    result = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return result[0] > 0


try:
    conn = get_connection()
except Exception as e:
    st.error(f"Could not connect to DuckDB at `{DB_PATH}`: {e}")
    st.stop()

# Check that dbt models have been run
if not has_table(conn, "on_time_by_route_hour"):
    st.warning(
        "No transformed data found. Run the following to get started:\n\n"
        "```bash\n"
        "python scripts/load_gtfs.py       # one-time\n"
        "python scripts/collect_data.py    # collect some data first\n"
        "python scripts/run_dbt.py         # build the mart tables\n"
        "```"
    )
    st.stop()

# ── Sidebar filters ──────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Filters")

    modes = conn.execute(
        "SELECT DISTINCT mode FROM on_time_by_route_hour ORDER BY mode"
    ).df()["mode"].tolist()
    selected_mode = st.selectbox("Mode", modes, index=0)

    routes = conn.execute(
        "SELECT DISTINCT route FROM on_time_by_route_hour WHERE mode = ? ORDER BY route",
        [selected_mode],
    ).df()["route"].tolist()
    selected_route = st.selectbox("Route", routes, index=0)

    day_options = ["All days", "Weekdays", "Weekends"]
    day_filter = st.selectbox("Day filter", day_options)

# ── Load data ────────────────────────────────────────────────────────────────

# Day-of-week filter: 0=Sun, 1=Mon … 5=Fri, 6=Sat
if day_filter == "Weekdays":
    day_clause = "AND day_of_week BETWEEN 1 AND 5"
elif day_filter == "Weekends":
    day_clause = "AND day_of_week IN (0, 6)"
else:
    day_clause = ""

heatmap_df: pd.DataFrame = conn.execute(f"""
    SELECT
        hour_of_day,
        day_name,
        day_of_week,
        sum(observation_count)  as observation_count,
        round(
            100.0 * sum(on_time_count) / sum(observation_count), 1
        ) as pct_on_time
    FROM on_time_by_route_hour
    WHERE mode = ? AND route = ?
    {day_clause}
    GROUP BY hour_of_day, day_name, day_of_week
    ORDER BY day_of_week, hour_of_day
""", [selected_mode, selected_route]).df()

history_df: pd.DataFrame = conn.execute("""
    SELECT
        date_trunc('day', collected_at) as day,
        mode,
        route,
        count(*) as arrivals,
        round(
            100.0 * sum(case when not is_delayed then 1 else 0 end) / count(*), 1
        ) as pct_on_time
    FROM arrival_history
    WHERE mode = ? AND route = ?
    GROUP BY 1, 2, 3
    ORDER BY 1
""", [selected_mode, selected_route]).df()

recent_df: pd.DataFrame = conn.execute("""
    SELECT
        collected_at,
        route,
        stop_name,
        destination,
        predicted_arrival,
        minutes_away,
        is_delayed
    FROM arrival_history
    WHERE mode = ? AND route = ?
    ORDER BY collected_at DESC
    LIMIT 100
""", [selected_mode, selected_route]).df()

# ── Summary metrics ───────────────────────────────────────────────────────────

st.subheader(f"{selected_mode.title()} {selected_route} — Overview")

total_obs = int(heatmap_df["observation_count"].sum()) if not heatmap_df.empty else 0
overall_pct = (
    round(
        100.0
        * heatmap_df["observation_count"]
        .mul(heatmap_df["pct_on_time"])
        .sum()
        / (heatmap_df["observation_count"].sum() * 100),
        1,
    )
    if total_obs > 0
    else None
)

col1, col2, col3 = st.columns(3)
col1.metric("Total observations", f"{total_obs:,}")
col2.metric("Overall on-time %", f"{overall_pct}%" if overall_pct is not None else "—")
col3.metric(
    "Days of data",
    f"{len(history_df)}" if not history_df.empty else "0",
)

st.divider()

# ── Heatmap ───────────────────────────────────────────────────────────────────

st.subheader("On-time % by hour and day")

if heatmap_df.empty:
    st.info("No data yet for this route. Collect more data and re-run dbt.")
else:
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    heatmap_pivot = (
        heatmap_df
        .pivot(index="day_name", columns="hour_of_day", values="pct_on_time")
        .reindex([d for d in day_order if d in heatmap_df["day_name"].values])
    )

    fig = px.imshow(
        heatmap_pivot,
        color_continuous_scale="RdYlGn",
        zmin=0,
        zmax=100,
        labels={"x": "Hour of day", "y": "", "color": "On-time %"},
        aspect="auto",
        text_auto=True,
    )
    fig.update_layout(
        coloraxis_colorbar_title="On-time %",
        xaxis=dict(
            tickmode="array",
            tickvals=list(range(24)),
            ticktext=[f"{h}:00" for h in range(24)],
        ),
        margin=dict(l=0, r=0, t=20, b=0),
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "Green = reliable, Red = frequent delays. "
        f"Based on {total_obs:,} arrival predictions."
    )

st.divider()

# ── Rolling on-time % line chart ──────────────────────────────────────────────

st.subheader("Daily on-time % over time")

if history_df.empty or len(history_df) < 2:
    st.info("Collect at least 2 days of data to see the trend chart.")
else:
    fig2 = px.line(
        history_df,
        x="day",
        y="pct_on_time",
        markers=True,
        labels={"day": "Date", "pct_on_time": "On-time %"},
    )
    fig2.update_layout(yaxis_range=[0, 100], margin=dict(l=0, r=0, t=10, b=0))
    fig2.add_hline(y=80, line_dash="dot", line_color="gray", annotation_text="80% threshold")
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── Recent history table ──────────────────────────────────────────────────────

st.subheader("Recent arrivals (last 100)")

if recent_df.empty:
    st.info("No recent arrivals found.")
else:
    recent_df["on_time"] = recent_df["is_delayed"].map({True: "Delayed", False: "On time"})
    st.dataframe(
        recent_df.drop(columns=["is_delayed"]).rename(columns={
            "collected_at": "Collected at",
            "route": "Route",
            "stop_name": "Stop",
            "destination": "Destination",
            "predicted_arrival": "Predicted arrival",
            "minutes_away": "Min away",
            "on_time": "Status",
        }),
        use_container_width=True,
        hide_index=True,
    )
