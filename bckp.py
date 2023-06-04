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
from dummy import *
#count = st_autorefresh(interval=2000, limit=100, key="fizzbuzzcounter")


channels = [2158069]
channel_keys = ["RWIHIZEWGEPQ5DEG"]
df_all_meter = pd.DataFrame(columns=['created_at','entry_id','field1','meter'])
dfs = []
for i in range(len(channels)):
    print(i)
    endpoint = f"https://api.thingspeak.com/channels/{channels[i]}/feeds.csv"

    channel_key = channel_keys[i]
    response = requests.get(endpoint, params={"api_key": channel_key})
    urlData = response.text
    df = pd.read_csv(io.StringIO(urlData))
    df['meter'] = channels[i]
   # print(df["created_at"])
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    group = df.groupby([df.created_at.dt.to_period('H'),df.meter])['field1'].sum()

    #    df['field1']= df['field1'].str.replace("/","").replace(".","")


    print(group)





print("reading data")


