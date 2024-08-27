import yfinance as yf
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

# Fetch historical stock data for Netflix and Apple
def fetch_stock_data(ticker, start_date, end_date):
    stock_data = yf.download(ticker, start=start_date, end=end_date)
    stock_data['Return'] = stock_data['Adj Close'].pct_change()
    stock_data.reset_index(inplace=True)
    
    # Generate a full date range for business days 
    all_dates = pd.date_range(start=start_date, end=end_date, freq='B')
    
    # Reindex stock_data to have this full date range
    stock_data = stock_data.set_index('Date').reindex(all_dates).rename_axis('Date').reset_index()
    
    # Handle missing values to make sure we have data for all business days
    stock_data.fillna(method='ffill', inplace=True)  # Forward fill
    stock_data.fillna(method='bfill', inplace=True)  # Backward fill
    
    return stock_data

# Remove outliers from the return data
def remove_outliers(df, column):
    q1 = df[column].quantile(0.25)
    q3 = df[column].quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    df_filtered = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
    return df_filtered

# Define start and end dates
start_date = '2024-01-01'
end_date = '2024-06-30'

# Fetch data
netflix_data = fetch_stock_data('NFLX', start_date, end_date)
apple_data = fetch_stock_data('AAPL', start_date, end_date)

# Remove outliers
netflix_data = remove_outliers(netflix_data, 'Return')
apple_data = remove_outliers(apple_data, 'Return')

# Display the first few rows of the data
print("Netflix Data:")
print(netflix_data.head())

print("\nApple Data:")
print(apple_data.head())

# Save the csv files
netflix_data.to_csv('netflix-prices.csv', index=False)
apple_data.to_csv('apple-prices.csv', index=False)

# Example: Plotting the closing prices of Netflix and Apple
plt.figure(figsize=(14, 7))

# Plot Netflix closing prices
plt.plot(netflix_data['Date'], netflix_data['Adj Close'], label='Netflix')

# Plot Apple closing prices
plt.plot(apple_data['Date'], apple_data['Adj Close'], label='Apple')

plt.title('Netflix and Apple Closing Prices')
plt.xlabel('Date')
plt.ylabel('Adjusted Closing Price')
plt.legend()
plt.grid(True)
plt.show()
