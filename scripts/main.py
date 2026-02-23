
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
        print("Ticker1: ", ticker)
        calculateMovingAverage(ticker)

def calculateMovingAverage(ticker):
    df = pd.read_parquet(dataFolder / f"{ticker}_data.parquet")
    print("Ticker: ", ticker)
    
main()