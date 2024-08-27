from gnews import GNews
import time
from datetime import datetime
import calendar
import pandas as pd

PROXY = ''

# Initialize GNews
google_news = GNews(language='en', country='US', max_results=100, proxy= PROXY)

# Function to retrieve news for a given date range
def get_news_for_month(year, month, ticker):
    # Calculate the start date as the first day of the month
    start_date = datetime(year, month, 1)
    
    # Calculate the last day of the month
    _, last_day = calendar.monthrange(year, month)
    end_date = datetime(year, month, last_day)
    
    google_news.start_date = (start_date.year, start_date.month, start_date.day)
    google_news.end_date = (end_date.year, end_date.month, end_date.day)
    
    return google_news.get_news(ticker)

def scrape_news(ticker):
    # Prepare to collect data
    all_articles = []
    
    # Set the starting date to June 30, 2024
    end_date = datetime(2024, 7, 19)

    # Loop through the past 12 months
    for i in range(12):
        # Calculate the year and month for the current iteration
        year = end_date.year if end_date.month - 1 - i > 0 else end_date.year - 1
        month = end_date.month - 1 - i if end_date.month - 1 - i > 0 else 12 + (end_date.month - 1 - i)
        
        # Retrieve news for the current month
        articles = get_news_for_month(year, month, ticker)
        all_articles.extend(articles)
        
        # Sleep to avoid hitting request limits
        time.sleep(60) 

    # Flatten the nested 'publisher' dictionary
    for article in all_articles:
        article.update(article.pop('publisher', {}))

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(all_articles)
    
    # Save the DataFrame as a CSV file
    df.to_csv(f'{ticker}-news.csv', index=False)

    print(f"Data retrieval for {ticker} completed and saved")

# Retrieve and save news articles for Apple and Netflix
scrape_news('aapl')
scrape_news('nflx')
