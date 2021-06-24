from pathlib import Path

import altair as alt
import streamlit as st
from pyspark.sql import SparkSession

from configs.configs import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

DATA_PATH = f"{Path(__file__).parent.parent.resolve().absolute()}/data"

spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.sql.shuffle.partitions", 6)
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .appName("tranform")
    .getOrCreate()
)

st.title("Pedestrian Counting System")


@st.cache
def load_data(filename):
    file_path = f"{DATA_PATH}/{filename}"
    data_df = spark.read.option("header", True).parquet(file_path)
    data = data_df.toPandas()
    data["latitude"] = data["latitude"].astype("float")
    data["longitude"] = data["longitude"].astype("float")

    return data


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
# st.bar_chart(res)

day_chart = (
    alt.Chart(res)
    .mark_bar()
    .encode(x="location", y="total_count", tooltip=["location", "total_count"])
)

st.altair_chart(day_chart, use_container_width=True)

st.subheader(f"Top 10 Locations Map for {day}")
st.map(data[data["day"] == day][["latitude", "longitude"]])


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

month_chart = (
    alt.Chart(res_m)
    .mark_bar()
    .encode(x="location", y="total_count", tooltip=["location", "total_count"])
)

st.altair_chart(month_chart, use_container_width=True)

st.subheader(f"Top 10 Locations Map for {month}")
st.map(data_m[data_m["month"] == month][["latitude", "longitude"]])
