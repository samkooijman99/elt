import yfinance as yf
import pandas as pd

def get_exchange_rates(start_date, end_date):
    df = yf.download("EURUSD=X", start=start_date, end=end_date)
    
    df = df[['Close']]
    
    df.columns = ['USD_per_EUR']
    
    df['EUR_per_USD'] = 1 / df['USD_per_EUR']
    
    df.columns = df.columns.str.lower()
    df = df.reset_index()
    
    # Convert data types, otherwise postgres won't accept it
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    
    for col in ['usd_per_eur', 'eur_per_usd']:
        df[col] = df[col].round(4).astype(float)
    
    return df