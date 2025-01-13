import pandas as pd
df = pd.read_csv(".spool/housing.csv")
print(df)
import streamlit as st
st.dataframe(df)

import matplotlib.pyplot as plt
import seaborn as sns

fig, ax = plt.subplots()
breakpoint()
sns.heatmap(df.corr(numeric_only=True), ax=ax)
st.write(fig)