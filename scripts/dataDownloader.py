import yfinance as yf
import pandas as pd
import requests
from pathlib import Path
from io import StringIO

#Getting path to data folder
currentDirectory = Path(__file__).resolve().parent
dataFolder = currentDirectory.parent / "data"
#create data folder if it doesn't exist (it won't upon download because the folder is gitignored)
dataFolder.mkdir(parents=True, exist_ok=True)



def pull_sp500_data():
    tickerList = []
    # pulling all tickers in S&P 500, probably would be easier to just download the data manually but its more fun to scrape it. 
    spWikipediaUrl = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    headers = { # Wikipedia didn't allow access without a defined user-agent, pulled this from MDN Docs
        'User-Agent': 'Mozilla/5.0 (platform; rv:gecko-version) Gecko/gecko-trail Firefox/firefox-version'
    } 
    # Because I have to add the header, using requests instead of just read_html the doc directly. 
    r = requests.get(spWikipediaUrl, headers=headers)

    tickerTable = pd.read_html(StringIO(r.text)) # Was getting an error when not using StringIO, apparently pandas prefers read_html uses a filelike object 
    print(tickerTable[0].head())

    # Save all the tickers used in S&P 500 to list
    tickerList = tickerTable[0]['Symbol'][0:5] # Testing with only 5
    
    # might change this to not create the file if it already exists BUT i plan on changing around the number of tickers for testing purposes and want to make sure it updates.
    tickerList.to_csv(dataFolder / 'ticker_list.csv', index=False, header=True)

    # Download data for each ticker from yfinance in parquet format. 
    for ticker in tickerList:

        filename = dataFolder / (ticker.lower() + "_data.parquet")
        if filename.exists():
            print(ticker + " data already downloaded, continuing.")
            continue

        print("Downloading: ", ticker)
        
        df = yf.download(
            ticker,

            period="20y",

            interval = "1d",

            auto_adjust=True,

            progress=False
        )

        save_individual_ticker_to_parquet(df, ticker, filename)


# save to parquet
# This just saves one stock to its own parquet file.

def save_individual_ticker_to_parquet(df, ticker, filename):
    # Normally, the columns are multilevel. Price "row" contains the various price columns: open, close, etc.
    # and "Ticker" holds the ticker, but we are using one ticker and saving the ticker name in the filename, thus, we can remove the ticker column
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.columns = [c.replace(' ', '_') for c in df.columns] # Replaces the " " in all columns. Not super useful here since the only " " is in adj close but good to be safe

    df.to_parquet(filename, engine="pyarrow", index=False)
    print("Saved to " + str(filename))

pull_sp500_data()