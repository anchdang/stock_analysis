import pandas as pd
import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import scipy
from transformers import pipeline
import re


start = '2023-07-19'
end = '2024-07-19'

# Function to process a dataframe
def process_df(df, date_column, date_format):
    # Convert the date column to datetime format and keep only month, day, and year
    df[date_column] = pd.to_datetime(df[date_column], format=date_format).dt.date
    return df

# Function to merge two dataframes based on date
def merge_dfs(df1, df2, date_column1, date_column2, start, end):
    # Select required columns
    df1 = df1[[date_column1, 'description']]
    df2 = df2[[date_column2, 'title']]
    
    # Rename columns to have the same name for merging
    df1 = df1.rename(columns={date_column1: 'date'})
    df2 = df2.rename(columns={date_column2: 'date'})
    
    # Merge dataframes on the date column
    merged_df = pd.merge(df1, df2, on='date', how='outer')
    
    # Drop duplicates
    merged_df = merged_df.drop_duplicates(subset=['date', 'title'])
    
    # Filter date range
    start_date = pd.to_datetime(start).date()
    end_date = pd.to_datetime(end).date()
    merged_df = merged_df[(merged_df['date'] >= start_date) & (merged_df['date'] <= end_date)]
    
    # drop the description column
    merged_df.drop(['description'], axis = 1, inplace = True)

    return merged_df

# Process AAPL data
aapl_news = pd.read_csv('aapl-news.csv')
aapl_business_news = pd.read_csv('aapl-business-news.csv')

aapl_news = process_df(aapl_news, 'published date', '%a, %d %b %Y %H:%M:%S GMT')
aapl_business_news = process_df(aapl_business_news, 'datetime', '%m/%d/%Y %I:%M:%S %p')

merged_aapl = merge_dfs(aapl_news, aapl_business_news, 'published date', 'datetime', start, end)

print("Merged AAPL Data:")
print(merged_aapl)


# Process NFLX data
nflx_news = pd.read_csv('nflx-news.csv')
nflx_business_news = pd.read_csv('nflx-business-news.csv')


nflx_news = process_df(nflx_news, 'published date', '%a, %d %b %Y %H:%M:%S GMT')
nflx_business_news = process_df(nflx_business_news, 'datetime', '%m/%d/%Y %I:%M:%S %p')


merged_nflx = merge_dfs(nflx_news, nflx_business_news, 'published date', 'datetime', start, end)

print("Merged NFLX Data:")
print(merged_nflx)

# Load the FinBERT model and tokenizer
tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")

# Create a sentiment analysis pipeline
finbert = pipeline('sentiment-analysis', model=model, tokenizer=tokenizer)

# Clean text function
def clean_text(text):
    text = re.sub(r"&amp;", "and", text)
    text = re.sub(r'\$', '', text)  # Remove $ sign before capitalized words
    text = re.sub(r'http\S+', '', text)  # Remove URLs
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)  # Remove non-alphanumeric characters
    text = re.sub(r'\s+', ' ', text)  # Remove extra whitespace
    return text.strip()

# Perform sentiment analysis
def analyze_sentiment(text):
    inputs = tokenizer(text, return_tensors="pt")
    with torch.no_grad():
        logits = model(**inputs).logits
    scores = {k: v for k, v in zip(model.config.id2label.values(), scipy.special.softmax(logits.numpy().squeeze()))}
    return scores

# Function to process dataframes and add sentiment analysis
def process_and_analyze(df):
    df['cleaned_title'] = df['title'].apply(clean_text)
    
    sentiments = df['cleaned_title'].apply(analyze_sentiment)
    
    # Extract sentiment scores
    df['neg'] = sentiments.apply(lambda x: x.get('negative', 0.0))
    df['neu'] = sentiments.apply(lambda x: x.get('neutral', 0.0))
    df['pos'] = sentiments.apply(lambda x: x.get('positive', 0.0))
    
    # Calculate the compound score (you can adjust this calculation if needed)
    df['compound'] = df['pos'] - df['neg']
    
    df.drop(['sentiment', 'title'], axis = 1, inplace = True)
    
    return df



# Process AAPL data
aapl_merged = process_and_analyze(merged_aapl)

print("AAPL Data with Sentiment Analysis:")
print(aapl_merged.head())

# Save the processed dataframe to a CSV file
aapl_merged.to_csv('aapl-news-sentiment.csv', index=False)

# Process NFLX data
nflx_merged = process_and_analyze(merged_nflx)

print("NFLX Data with Sentiment Analysis:")
print(nflx_merged.head())

# Save the processed dataframe to a CSV file
nflx_merged.to_csv('nflx-news-sentiment.csv', index=False)


