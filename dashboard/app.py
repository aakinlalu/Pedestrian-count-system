from pathlib import Path

import pandas as pd
import altair as alt
import streamlit as st

from configs.configs import spark

DATA_PATH = f"{Path(__file__).parent.parent.resolve().absolute()}/data"

st.title("Pedestrian Counting System")


@st.cache
def load_data(filename: str) -> pd.DataFrame:
    """"""
    file_path = f"{DATA_PATH}/{filename}"
    data_df = spark.read.option("header", True).parquet(file_path)
    data = data_df.toPandas()
    data["latitude"] = data["latitude"].astype("float")
    data["longitude"] = data["longitude"].astype("float")

    return data


def chart(data: pd.DataFrame):
    return (
        alt.Chart(data, height=500)
        .mark_bar()
        .encode(x="total_count", y="location", tooltip=["location", "total_count"])
    )


# Day Report
st.header("Top Ten Locations by Day")
start_loading = st.text("loading Top Ten location by Day data....")
data = load_data("top_10_loc_by_day")
start_loading.text("Top Ten locations by Day data...Done")

if st.checkbox("Show Top 10 locations by day"):
    st.subheader("Tabular Data")
    st.write(data)

day = st.selectbox("What day do you want to see?", data["day"].unique())

st.subheader(f"Top 10 Locations Chart for {day}")
res = data[data["day"] == day][["location", "total_count"]]
day_chart = chart(res)
st.altair_chart(day_chart, use_container_width=True)

st.subheader(f"Top 10 Locations Map for {day}")
st.map(data[data["day"] == day][["latitude", "longitude"]])


# Month Report
st.header("Top Ten Locations by month")
start_loading = st.text("loading Top Ten locations by Month data....")
data_m = load_data("top_10_loc_by_month")
start_loading.text("Top Ten location by Month data...Done")

if st.checkbox("Show Top 10 locations by month"):
    st.subheader("Tabular Data")
    st.write(data_m)

month = st.selectbox("What month you want to see?", data_m["month"].unique())
st.subheader(f"Top 10 Locations Chart for {month}")
res_m = data_m[data_m["month"] == month][["location", "total_count"]]
month_chart = chart(res_m)
st.altair_chart(month_chart, use_container_width=True)

st.subheader(f"Top 10 Locations Map for {month}")
st.map(data_m[data_m["month"] == month][["latitude", "longitude"]])
