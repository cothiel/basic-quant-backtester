import yfinance as yf
import pandas as pd
from pathlib import Path


# Download ticker data into pandas dataframe
ticker = "AAPL"

# Using yfinance, remember it has survivorship bias
df = yf.download(

    ticker,

    start="2000-01-01",

    auto_adjust=False,

    progress=False

)


# save to parquet
# This just saves one stock to its own parquet file.
# May change to a partitioned dataset instead, that way if I want to do a strategy that uses the entire S&P 500 for example I don't have to open and close 500+ files
def save_individual_ticker_to_parquet(df, ticker, filename=ticker.lower() + "_data.parquet"):
    #Getting path to data folder
    currentDirectory = Path(__file__).resolve().parent
    dataFolder = currentDirectory.parent / "data"   
    dataFolder.mkdir(parents=True, exist_ok=True)
    filename = dataFolder / filename
    #create data folder if it doesn't exist (it won't upon download because the folder is gitignored)


    # Normally, the columns are multilevel. Price "row" contains the various price columns: open, close, etc.
    # and "Ticker" holds the ticker, but we are using one ticker and saving the ticker name in the filename, thus, we can remove the ticker column
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.columns = [c.replace(' ', '_') for c in df.columns] # Replaces the " " in all columns. Not super useful here since the only " " is in adj close but good to be safe

    df.to_parquet(filename, engine="pyarrow", index=False)
    print("Saved to " + str(filename))


save_individual_ticker_to_parquet(df, ticker)