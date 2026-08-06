from kafka import KafkaProducer
import yfinance as yf
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

stocks = [
    "TCS.NS",
    "INFY.NS",
    "HDFCBANK.NS",
    "ICICIBANK.NS",
    "SBIN.NS",
    "ITC.NS",
    "LT.NS",
    "AXISBANK.NS",
    "BHARTIARTL.NS",
    "RELIANCE.NS"
]

print("Producer Started...")

while True:

    for stock in stocks:

        try:

            df = yf.download(
                tickers=stock,
                period="1d",
                interval="1m",
                progress=False,
                auto_adjust=False,
                group_by="column",
                threads=False
            )

            if df.empty:
                continue

            # MultiIndex hata do agar ho
            if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
                df.columns = df.columns.get_level_values(0)

            last = df.iloc[-1]
            ts = df.index[-1]

            data = {
                "date": ts.strftime("%Y-%m-%d"),
                "time": ts.strftime("%H:%M:%S"),
                "symbol": stock,
                "price": round(float(last["Close"]), 2),
                "open": round(float(last["Open"]), 2),
                "high": round(float(last["High"]), 2),
                "low": round(float(last["Low"]), 2),
                "volume": int(last["Volume"])
            }

            producer.send("nifty-topic", value=data)
            producer.flush()

            print(data)

        except Exception as e:
            print("ERROR:", stock, e)

    print("\nWaiting 60 seconds...\n")
    time.sleep(60)
