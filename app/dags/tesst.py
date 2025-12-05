
import pandas as pd
import requests

def extract_currency_data():
    data = {}
    response = requests.get("https://free.ratesdb.com/v1/rates", params={"from": "INR"})
    if response.status_code == 200:
        data = response.json()["data"]
        print(data)
        df = pd.DataFrame(data['rates'].items(), columns=['currency_code', 'rate'])
        df["date"] = data["date"]
        df["inverse_rate"] = 1/df["rate"]
        df = df[["date", "currency_code", "rate", "inverse_rate"]]
        # ti.xcom_push(key='currency_payload',value={
        #         "date": str(data["date"]),
        #         "currency_data": df.to_dict('records')})
        print("*******************", data["date"])
        print("&&&&&&&&&&&&&&77",df.to_dict('records'))


extract_currency_data()