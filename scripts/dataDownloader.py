import yfinance as yf
import pandas as pd


df = yf.download(

    "AAPL",

    start="2000-01-01",

    auto_adjust=False,

    progress=False

)



print(df.head())
print(df.shape)
