import pandas as pd
import re
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import demoji
from datetime import datetime
import nltk
import html

# Download the VADER lexicon
nltk.download('vader_lexicon')

# Initialize demoji
demoji.download_codes() 

# Add new words to VADER
new_words = {
    'citron': -4.0,  
    'hidenburg': -4.0,        
    'moon': 4.0,
    'highs': 2.0,
    'mooning': 4.0,
    'long': 2.0,
    'short': -2.0,
    'call': 4.0,
    'calls': 4.0,    
    'put': -4.0,
    'puts': -4.0,    
    'break': 2.0,
    'tendie': 2.0,
    'tendies': 2.0,
    'town': 2.0,     
    'overvalued': -3.0,
    'undervalued': 3.0,
    'buy': 4.0,
    'sell': -4.0,
    'gone': -1.0,
    'gtfo': -1.7,
    'paper': -1.7,
    'bullish': 3.7,
    'bearish': -3.7,
    'bagholder': -1.7,
    'stonk': 1.9,
    'green': 1.9,
    'money': 1.2,
    'print': 2.2,
    'rocket': 2.2,
    'bull': 2.9,
    'bear': -2.9,
    'pumping': -1.0,
    'sus': -3.0,
    'offering': -2.3,
    'rip': -4.0,
    'downgrade': -3.0,
    'upgrade': 3.0,     
    'maintain': 1.0,          
    'pump': 1.9,
    'hot': 1.5,
    'drop': -2.5,
    'rebound': 1.5,  
    'crack': 2.5
}

def convert_emojis(text):
    # Convert emojis to text using demoji
    return demoji.replace(text, "")

def clean_text(text):
    # Convert HTML entities to their respective characters
    text = html.unescape(text)
    
    # Convert emojis to text
    text = convert_emojis(text)
    
    # Remove URLs
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    
    # Remove mentions
    text = re.sub(r'@\w+', '', text)
    
    # Remove hashtags
    text = re.sub(r'#\w+', '', text)
    
    # Remove $ before the ticker
    text = re.sub(r'\$(?=[A-Z]{2,})', '', text)
    
    # Remove special characters and digits
    #text = re.sub(r'[^A-Za-z\s]', '', text)
    
    return text

# Function to perform sentiment analysis using VADER
def perform_sentiment_analysis(path):
    sia = SentimentIntensityAnalyzer()
    # Retrieve dataframe
    df = pd.read_csv(path)
    # Add new words to VADER
    sia.lexicon.update(new_words)
    df['cleaned_text'] = df['title'].apply(clean_text)
    sentiments = df['cleaned_text'].apply(sia.polarity_scores)
    df = pd.concat([df, sentiments.apply(pd.Series)], axis=1)
    df['date'] = pd.to_datetime(df['date'], unit='s')
    df = df[['date', 'cleaned_text', 'neg', 'neu', 'pos', 'compound']]
    df.to_csv(f'sentiment-{path}', index=False)
    return df

perform_sentiment_analysis('aapl-subreddits.csv')
perform_sentiment_analysis('nflx-subreddits.csv')

print("Data processed and sentiment analysis completed.")
