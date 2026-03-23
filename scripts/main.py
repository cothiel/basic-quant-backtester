
from pathlib import Path
import pandas as pd




currentDirectory = Path(__file__).resolve().parent
dataFolder = currentDirectory.parent / "data"

def main():
    # get ticker list
    tickers = pd.read_csv(dataFolder / 'ticker_list.csv')
    tickers = tickers['Symbol'].tolist()
    testDate = '1/1/2020'
    print(tickers)
    for ticker in tickers:
        calculateMovingAverage(ticker)

def calculateMovingAverage(ticker):
    df = pd.read_parquet(dataFolder / f"{ticker}_data.parquet")
    
    print("Ticker: ", ticker)
    df['SMA'] = df['Close'].rolling(window=20).mean().fillna(0)
    df['SD'] = df['Close'].rolling(window=20).std().fillna(0)
    print(df['SD'])
main()