import yfinance as yf
import os
import pandas as pd

dfs = []

def get_stock_data():
    for isin in os.getenv('TICKER_SYMBOLS'):
        ticker = yf.Ticker(isin)
        historical_data = ticker.history(period="20y")

        historical_data['Symbol'] = isin
        dfs.append(historical_data)
        
    combined_df = pd.concat(dfs, axis=0)
    
    # Reset the index to have date and symbol as columns
    combined_df = combined_df.reset_index()

    combined_df = combined_df.rename(columns={
        'Adj Close': 'Adj_Close',
        'Stock Splits': 'Stock_Splits'
    })
    
    # Sort by Date and Symbol
    combined_df = combined_df.sort_values(['Date', 'Symbol'])
    
    print(f"\nFinal DataFrame shape: {combined_df.shape}")
    return combined_df