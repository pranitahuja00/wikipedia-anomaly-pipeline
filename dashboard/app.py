import os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from databricks import sql
from datetime import datetime, timedelta

st.set_page_config(
    page_title="Wikipedia Edit Anomaly Dashboard",
    page_icon="📊",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Connection — credentials injected by Databricks App runtime
# ---------------------------------------------------------------------------

@st.cache_resource
def get_workspace_client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


def _get_warehouse_http_path() -> str:
    if os.environ.get("DATABRICKS_WAREHOUSE_HTTP_PATH"):
        return os.environ["DATABRICKS_WAREHOUSE_HTTP_PATH"]
    w = get_workspace_client()
    return w.secrets.get_secret(
        scope="wikipedia-anomaly-pipeline", key="warehouse-http-path"
    ).value


@st.cache_resource
def get_connection():
    w = get_workspace_client()
    return sql.connect(
        server_hostname=w.config.host,
        http_path=_get_warehouse_http_path(),
        access_token=w.config.token,
    )


@st.cache_data(ttl=120)
def query(sql_text: str) -> pd.DataFrame:
    with get_connection().cursor() as cur:
        cur.execute(sql_text)
        return cur.fetchall_arrow().to_pandas()


# ---------------------------------------------------------------------------
# Sidebar — filters
# ---------------------------------------------------------------------------

st.sidebar.title("Filters")

window_choice = st.sidebar.selectbox(
    "Window granularity",
    options=["1min", "5min"],
    index=1,
)

lookback_hours = st.sidebar.slider(
    "Lookback (hours)",
    min_value=1,
    max_value=48,
    value=6,
)

auto_refresh = st.sidebar.checkbox("Auto-refresh every 2 min", value=False)

if auto_refresh:
    import time
    st.sidebar.caption(f"Last refresh: {datetime.utcnow().strftime('%H:%M:%S')} UTC")

since_ts = (datetime.utcnow() - timedelta(hours=lookback_hours)).strftime("%Y-%m-%d %H:%M:%S")

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

volume_sql = f"""
SELECT
    window_start,
    window_end,
    total_edits,
    bot_edits,
    human_edits,
    bot_ratio,
    unique_users,
    total_bytes_changed,
    avg_bytes_per_edit,
    revert_count,
    is_anomaly,
    z_score,
    rolling_mean_edits,
    anomaly_threshold
FROM gold_edit_volume
WHERE window_duration = '{window_choice}'
  AND window_start >= '{since_ts}'
ORDER BY window_start
"""

user_sql = f"""
SELECT
    window_start,
    user,
    bot,
    edit_count,
    bytes_changed,
    revert_count,
    unique_pages_edited
FROM gold_user_activity
WHERE window_duration = '{window_choice}'
  AND window_start >= '{since_ts}'
ORDER BY window_start, edit_count DESC
"""

with st.spinner("Loading data..."):
    try:
        vol_df  = query(volume_sql)
        user_df = query(user_sql)
        data_ok = True
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        data_ok = False

if not data_ok or vol_df.empty:
    st.warning("No data in the selected time window. Try extending the lookback or check that the pipeline is running.")
    st.stop()

# Ensure datetime types
vol_df["window_start"] = pd.to_datetime(vol_df["window_start"])
vol_df["window_end"]   = pd.to_datetime(vol_df["window_end"])
user_df["window_start"] = pd.to_datetime(user_df["window_start"])

anomaly_df = vol_df[vol_df["is_anomaly"]]

# ---------------------------------------------------------------------------
# Summary metrics
# ---------------------------------------------------------------------------

st.title("Wikipedia Edit Stream — Anomaly Dashboard")
st.caption(f"Showing last {lookback_hours}h · {window_choice} windows · {len(vol_df):,} windows loaded")

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Edits",       f"{vol_df['total_edits'].sum():,}")
c2.metric("Anomalous Windows", f"{anomaly_df.shape[0]:,}",
          delta=f"{anomaly_df.shape[0] / len(vol_df) * 100:.1f}% of windows",
          delta_color="inverse")
c3.metric("Avg Bot Ratio",     f"{vol_df['bot_ratio'].mean():.1%}")
c4.metric("Peak Edits/Window", f"{vol_df['total_edits'].max():,}")
c5.metric("Unique Users",      f"{user_df['user'].nunique():,}")

st.divider()

# ---------------------------------------------------------------------------
# Chart 1 — Edit volume time series with anomaly flags
# ---------------------------------------------------------------------------

st.subheader("Edit Volume — Anomaly Flags")

fig_vol = go.Figure()

fig_vol.add_trace(go.Scatter(
    x=vol_df["window_start"],
    y=vol_df["total_edits"],
    mode="lines",
    name="Total Edits",
    line=dict(color="#4C9BE8", width=1.5),
))

fig_vol.add_trace(go.Scatter(
    x=vol_df["window_start"],
    y=vol_df["rolling_mean_edits"],
    mode="lines",
    name="Rolling Mean",
    line=dict(color="#A8D8A8", width=1, dash="dot"),
))

fig_vol.add_trace(go.Scatter(
    x=vol_df["window_start"],
    y=vol_df["anomaly_threshold"],
    mode="lines",
    name="Anomaly Threshold (2σ)",
    line=dict(color="#F4A460", width=1, dash="dash"),
    fill=None,
))

if not anomaly_df.empty:
    fig_vol.add_trace(go.Scatter(
        x=anomaly_df["window_start"],
        y=anomaly_df["total_edits"],
        mode="markers",
        name="Anomaly",
        marker=dict(color="#FF4B4B", size=8, symbol="circle-open", line=dict(width=2)),
    ))

fig_vol.update_layout(
    height=350,
    margin=dict(l=0, r=0, t=10, b=0),
    legend=dict(orientation="h", yanchor="bottom", y=1.02),
    xaxis_title=None,
    yaxis_title="Edits per window",
)
st.plotly_chart(fig_vol, use_container_width=True)

# ---------------------------------------------------------------------------
# Chart 2 — Bot ratio trend
# ---------------------------------------------------------------------------

st.subheader("Bot vs Human Edit Ratio")

fig_bot = go.Figure()

fig_bot.add_trace(go.Bar(
    x=vol_df["window_start"],
    y=vol_df["human_edits"],
    name="Human",
    marker_color="#4C9BE8",
))
fig_bot.add_trace(go.Bar(
    x=vol_df["window_start"],
    y=vol_df["bot_edits"],
    name="Bot",
    marker_color="#FF8C42",
))
fig_bot.add_trace(go.Scatter(
    x=vol_df["window_start"],
    y=vol_df["bot_ratio"],
    mode="lines",
    name="Bot Ratio",
    yaxis="y2",
    line=dict(color="#9B59B6", width=1.5),
))

fig_bot.update_layout(
    barmode="stack",
    height=300,
    margin=dict(l=0, r=0, t=10, b=0),
    legend=dict(orientation="h", yanchor="bottom", y=1.02),
    xaxis_title=None,
    yaxis=dict(title="Edits"),
    yaxis2=dict(title="Bot Ratio", overlaying="y", side="right",
                tickformat=".0%", range=[0, 1]),
)
st.plotly_chart(fig_bot, use_container_width=True)

# ---------------------------------------------------------------------------
# Chart 3 — Z-score over time
# ---------------------------------------------------------------------------

st.subheader("Z-Score — Anomaly Severity")

fig_z = go.Figure()

fig_z.add_hrect(
    y0=2.0, y1=vol_df["z_score"].max() * 1.1 if vol_df["z_score"].max() > 2 else 3,
    fillcolor="rgba(255, 75, 75, 0.1)",
    line_width=0,
    annotation_text="Anomaly zone (z > 2)",
    annotation_position="top left",
)
fig_z.add_hline(y=2.0, line_dash="dash", line_color="#FF4B4B", line_width=1)

fig_z.add_trace(go.Scatter(
    x=vol_df["window_start"],
    y=vol_df["z_score"],
    mode="lines+markers",
    name="Z-Score",
    line=dict(color="#4C9BE8", width=1.5),
    marker=dict(size=3),
))

fig_z.update_layout(
    height=260,
    margin=dict(l=0, r=0, t=10, b=0),
    xaxis_title=None,
    yaxis_title="Z-Score",
    showlegend=False,
)
st.plotly_chart(fig_z, use_container_width=True)

# ---------------------------------------------------------------------------
# Chart 4 — Top users during anomalous windows
# ---------------------------------------------------------------------------

st.subheader("Top Editors During Anomalous Windows")

if anomaly_df.empty:
    st.info("No anomalous windows in the selected time range.")
else:
    anomaly_windows = set(anomaly_df["window_start"].dt.floor("min"))
    anomaly_users = user_df[user_df["window_start"].dt.floor("min").isin(anomaly_windows)]

    top_users = (
        anomaly_users
        .groupby(["user", "bot"])
        .agg(edit_count=("edit_count", "sum"),
             windows=("window_start", "nunique"),
             unique_pages=("unique_pages_edited", "sum"))
        .reset_index()
        .sort_values("edit_count", ascending=False)
        .head(20)
    )

    top_users["label"] = top_users["user"] + top_users["bot"].map({True: " 🤖", False: ""})

    fig_users = px.bar(
        top_users,
        x="edit_count",
        y="label",
        orientation="h",
        color="bot",
        color_discrete_map={True: "#FF8C42", False: "#4C9BE8"},
        labels={"edit_count": "Total Edits", "label": "User", "bot": "Bot"},
        height=max(300, len(top_users) * 22),
    )
    fig_users.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        yaxis=dict(autorange="reversed"),
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig_users, use_container_width=True)

# ---------------------------------------------------------------------------
# Raw anomaly table
# ---------------------------------------------------------------------------

with st.expander("Raw anomaly windows"):
    display_cols = [
        "window_start", "window_end", "total_edits", "bot_edits",
        "human_edits", "bot_ratio", "z_score", "revert_count",
    ]
    st.dataframe(
        anomaly_df[display_cols].sort_values("window_start", ascending=False),
        use_container_width=True,
        hide_index=True,
    )

# ---------------------------------------------------------------------------
# Auto-refresh
# ---------------------------------------------------------------------------

if auto_refresh:
    import time
    time.sleep(120)
    st.cache_data.clear()
    st.rerun()
