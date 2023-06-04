import streamlit as st # web development
import numpy as np
import io
import pandas as pd
import snowflake.connector as sf
import requests
import time
import pandas as pd
import plotly.express as px # interactive charts
from streamlit_autorefresh import st_autorefresh
from schedule import every, repeat, run_pending
import dummy
#count = st_autorefresh(interval=2000, limit=100, key="fizzbuzzcounter")
@repeat(every(5).seconds)
def get_data():
    channels = [2158069,2163284,2163450]
    channel_keys = ["RWIHIZEWGEPQ5DEG","LZ4XXB5EXY86LZFU","OH3A1IZ338G34QSQ"]
    df_all_meter = pd.DataFrame(columns=['created_at','entry_id','field1','meter'])
    dfs = []
    for i in range(len(channels)):
     #   print(i)
        endpoint = f"https://api.thingspeak.com/channels/{channels[i]}/feeds.csv"

        channel_key = channel_keys[i]
        response = requests.get(endpoint, params={"api_key": channel_key})
        urlData = response.text
        df = pd.read_csv(io.StringIO(urlData))
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['meter'] = channels[i]
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
      #  group = df.groupby([df.created_at.dt.to_period('H'),df.meter])['field1'].sum()
        dfs.append(df)
    df_all_meter = pd.concat(dfs)
 #   print("reading data")
    return  df_all_meter


df = get_data()
st.set_page_config(
    page_title='Real-Time Data Science Dashboard',
    page_icon='✅',
    layout='wide'
)

# dashboard title

st.title("Real-Time / Live Data Science Dashboard")

# top-level filters

job_filter = st.selectbox("Select the meter", pd.unique(df['meter']))

# creating a single-element container.
placeholder = st.empty()

# dataframe filter

df = df[df['meter'] == job_filter]

# near real-time / live feed simulation

for seconds in range(2):
  #  print("in loop")

    # while True:

    # df['age_new'] = df['age'] * np.random.choice(range(1, 5))
    # df['balance_new'] = df['balance'] * np.random.choice(range(1, 5))
    #
    # # creating KPIs
    # avg_age = np.mean(df['age_new'])
    #
    # count_married = int(df[(df["marital"] == 'married')]['marital'].count() + np.random.choice(range(1, 30)))
    #
    # balance = np.mean(df['balance_new'])

    with placeholder.container():
        # # create three columns
        # kpi1, kpi2, kpi3 = st.columns(3)
        #
        # # fill in those three columns with respective metrics or KPIs
        # kpi1.metric(label="Age ⏳", value=round(avg_age), delta=round(avg_age) - 10)
        # kpi2.metric(label="Married Count 💍", value=int(count_married), delta=- 10 + count_married)
        # kpi3.metric(label="A/C Balance ＄", value=f"$ {round(balance, 2)} ",
        #             delta=- round(balance / count_married) * 100)

        # create two columns for charts

        fig_col1, fig_col2 = st.columns(2)
        with fig_col1:
            st.markdown("### First Chart")
            fig = px.line(data_frame=df, y='field1', x='created_at')
            st.write(fig)
        with fig_col2:
            st.markdown("### Second Chart")
            fig2 = px.histogram(data_frame=df, x='field1')
            st.write(fig2)
        st.markdown("### Detailed Data View")
        st.dataframe(df)
        time.sleep(1)
    # placeholder.empty()
