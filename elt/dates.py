from datetime import datetime, timedelta
import pandas as pd

def get_dates():
    
    end_date = datetime.now()
    start_date = end_date - timedelta(365.25 * 20)

    # Create a list to store all dates
    dates_list = []
    current_date = start_date
    
    # Generate all dates between start and end
    while current_date <= end_date:
        dates_list.append(current_date.date())
        current_date += timedelta(days=1)
    
    # Create a df
    dates_as_strings = [date.strftime("%Y-%m-%d") for date in dates_list]
    df = pd.DataFrame(dates_as_strings, columns=['date'])
    return df
