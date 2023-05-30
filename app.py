import snowflake.connector as sf
import requests
import time
import streamlit as st
import pandas as pd
st.write("IOT Smart Energy")
endpoint = "https://api.thingspeak.com/channels/2158069/feeds.json"
channel_key = "RWIHIZEWGEPQ5DEG"
channels = [2158069]
keys = ["RWIHIZEWGEPQ5DEG"]
endpoint = "https://api.thingspeak.com/channels/2158069/feeds.json"
channel_key = "RWIHIZEWGEPQ5DEG"
response = requests.get(endpoint, params={"api_key": channel_key})
df = pd.DataFrame(columns = ['Date', 'meter_id', 'entry_id', 'power'])
sf_conn_obj = sf.connect(
    user='BKARAPINAR',
    password='Metin35qqwe.',
    account='fq22210.europe-west4.gcp',
    warehouse='COMPUTE_WH',
    database='POWER_IOT',
    schema='TRANSACTION_POWER'
    )

sf_cursor_obj = sf_conn_obj.cursor()
#my_chart = st.dataframe(df)
while True:

    try:
        sf_cursor_obj = sf_conn_obj.cursor()
        for i in range(len(channels)):
            print("next channel"  )
            endpoint = f"https://api.thingspeak.com/channels/{channels[i]}/feeds.json"
            channel_key = keys[i]
            response = requests.get(endpoint, params={"api_key": channel_key})

            if response.status_code == 200:
                data = response.json()


                feeds = data["feeds"]
                for feed in feeds:
                    field1_value = feed["field1"].split('/')
                    date = feed["created_at"]
                    meter_id = field1_value[0]
                    entry_id = feed["entry_id"]
                    power = field1_value[1]
                    sf_cursor_obj.execute(
                        f"select count(*) from POWER_IOT.TRANSACTION_POWER.TRANSACTION_POWER where meter_id = {meter_id} and entry_id = {entry_id};")
                    one_row = sf_cursor_obj.fetchone()
                    if one_row[0] == 0:
                        sf_cursor_obj.execute(f"INSERT INTO POWER_IOT.TRANSACTION_POWER.TRANSACTION_POWER VALUES (\'{date}\',{meter_id},{entry_id},{power} )"
                                              )
                    sf_cursor_obj.execute(
                        f"select * from POWER_IOT.TRANSACTION_POWER.TRANSACTION_POWER ;")
                    one_row = sf_cursor_obj.fetchone()
                    #print(one_row)
                    # df = df.append({'Date': date, 'meter_id': meter_id, 'entry_id': entry_id, 'power':power},
                    #                ignore_index=True)

                    new_row = {'Date': date, 'meter_id': meter_id, 'entry_id': entry_id, 'power':power}

                    # Use the loc method to add the new row to the DataFrame
                    df.loc[len(df)] = new_row
                    my_chart = st.dataframe(df)
                    st.empty()
            else:
                print("error:", response.status_code)
    finally:
        sf_cursor_obj.close()
    sf_cursor_obj.close()
    print("done step")
    time.sleep(5)