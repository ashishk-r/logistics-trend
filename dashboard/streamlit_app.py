from pathlib import Path

import pandas as pd
import streamlit as st


st.set_page_config(
    page_title="Transportation & Logistics Trends",
    page_icon="🚚",
    layout="wide",
)

DATA_DIR = Path(__file__).parent / "data"


@st.cache_data
def read_spark_output(folder_name: str) -> pd.DataFrame:
    folder = DATA_DIR / folder_name
    files = sorted(folder.glob("part-*.csv")) if folder.exists() else []
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_csv(path) for path in files), ignore_index=True)


st.title("Transportation and Logistics Trend Analysis")
st.caption("Data processed with Scala and Apache Spark · Interactive view built with Streamlit")

kpis = read_spark_output("overall_kpis")
segments = read_spark_output("segment_monthly")
monthly_all = read_spark_output("monthly_trends")
mode_all = read_spark_output("shipping_mode_summary")
region_all = read_spark_output("region_summary")
status_all = read_spark_output("status_summary")

if kpis.empty or segments.empty:
    st.info(
        "The dashboard is ready, but it has no Spark summaries yet. Download the DataCo CSV, "
        "run the Scala/Spark job, and place its generated summary folders in `dashboard/data/`. "
        "See the project README for the steps."
    )
    st.stop()

summary = kpis.iloc[0]
metric_cols = st.columns(4)
metric_cols[0].metric("Completed orders", f"{int(summary['completed_orders']):,}")
metric_cols[1].metric("Late orders", f"{int(summary['late_orders']):,}")
metric_cols[2].metric("Late delivery rate", f"{summary['late_rate_pct']:.1f}%")
metric_cols[3].metric("Cancelled orders", f"{int(summary['cancelled_orders']):,}")

st.caption(
    f"Average actual shipping time: {summary['avg_actual_days']:.2f} days · "
    f"Average scheduled shipping time: {summary['avg_scheduled_days']:.2f} days · "
    f"Average excess time per completed order: {summary['avg_excess_days']:.2f} days"
)

st.subheader("Explore delivery trends")
filter_cols = st.columns(3)
markets = ["All markets"] + sorted(segments["market"].dropna().astype(str).unique().tolist())
regions = ["All regions"] + sorted(segments["region"].dropna().astype(str).unique().tolist())
modes = ["All shipping modes"] + sorted(segments["shipping_mode"].dropna().astype(str).unique().tolist())
market_choice = filter_cols[0].selectbox("Market", markets)
region_choice = filter_cols[1].selectbox("Region", regions)
mode_choice = filter_cols[2].selectbox("Shipping mode", modes)

filtered = segments.copy()
if market_choice != "All markets":
    filtered = filtered[filtered["market"].astype(str) == market_choice]
if region_choice != "All regions":
    filtered = filtered[filtered["region"].astype(str) == region_choice]
if mode_choice != "All shipping modes":
    filtered = filtered[filtered["shipping_mode"].astype(str) == mode_choice]

if filtered.empty:
    st.warning("There are no orders for this filter combination.")
else:
    numeric_columns = [
        "total_orders", "late_orders", "avg_actual_days", "avg_scheduled_days", "avg_excess_days"
    ]
    for name in numeric_columns:
        filtered[name] = pd.to_numeric(filtered[name], errors="coerce").fillna(0)
    filtered["actual_weighted"] = filtered["avg_actual_days"] * filtered["total_orders"]
    filtered["scheduled_weighted"] = filtered["avg_scheduled_days"] * filtered["total_orders"]
    filtered["excess_weighted"] = filtered["avg_excess_days"] * filtered["total_orders"]

    monthly = filtered.groupby("month", as_index=False).agg(
        total_orders=("total_orders", "sum"),
        late_orders=("late_orders", "sum"),
        actual_weighted=("actual_weighted", "sum"),
        scheduled_weighted=("scheduled_weighted", "sum"),
        excess_weighted=("excess_weighted", "sum"),
    ).sort_values("month")
    monthly["late_rate_pct"] = monthly["late_orders"] / monthly["total_orders"] * 100
    monthly["avg_actual_days"] = monthly["actual_weighted"] / monthly["total_orders"]
    monthly["avg_scheduled_days"] = monthly["scheduled_weighted"] / monthly["total_orders"]
    monthly["avg_excess_days"] = monthly["excess_weighted"] / monthly["total_orders"]

    chart_cols = st.columns(2)
    with chart_cols[0]:
        st.markdown("**Late delivery rate by month**")
        st.line_chart(monthly.set_index("month")["late_rate_pct"], y_label="Late orders (%)")
    with chart_cols[1]:
        st.markdown("**Average shipping time by month**")
        st.line_chart(
            monthly.set_index("month")[["avg_actual_days", "avg_scheduled_days"]],
            y_label="Days",
        )

    st.markdown("**Monthly metrics for the selected filters**")
    st.dataframe(
        monthly[["month", "total_orders", "late_orders", "late_rate_pct", "avg_actual_days", "avg_scheduled_days", "avg_excess_days"]],
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Shipping mode comparison")
if not mode_all.empty:
    mode_display = mode_all.copy()
    mode_display["late_rate_pct"] = pd.to_numeric(mode_display["late_rate_pct"], errors="coerce")
    st.bar_chart(mode_display.set_index("shipping_mode")["late_rate_pct"], y_label="Late orders (%)")
    st.dataframe(mode_display, use_container_width=True, hide_index=True)

st.subheader("Regional summary")
if not region_all.empty:
    st.dataframe(region_all, use_container_width=True, hide_index=True)

with st.expander("Delivery status mix"):
    if not status_all.empty:
        st.bar_chart(status_all.set_index("delivery_status")["orders"])
        st.dataframe(status_all, use_container_width=True, hide_index=True)

st.divider()
st.caption(
    "Source: DataCo SMART SUPPLY CHAIN FOR BIG DATA ANALYSIS, Mendeley Data, CC BY 4.0. "
    "The dashboard displays aggregated results only."
)
