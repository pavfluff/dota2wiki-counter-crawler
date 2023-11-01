import streamlit as st
import pandas as pd

st.title("DotA 2 Hero Counter")

df = pd.read_parquet("/data/*/*.parquet")