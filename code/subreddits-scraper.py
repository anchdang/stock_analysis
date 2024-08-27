import requests
from pprint import pprint
import sqlite3
import pandas as pd

PROXY = ''

# Connect to the database and create a table
def create_tables(conn, keyword, subreddit, sort_type):
    c = conn.cursor()
    table_name = f'{keyword}_{subreddit}_{sort_type}'
    c.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id TEXT PRIMARY KEY,
            title TEXT,
            score INTEGER,
            author TEXT,
            date REAL,
            url TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS state (
            table_name TEXT PRIMARY KEY,
            after TEXT
        )
    ''')
    conn.commit()

# Start crawling from the last result
def get_last_after(conn, table_name):
    c = conn.cursor()
    c.execute('SELECT after FROM state WHERE table_name=?', (table_name,))
    res = c.fetchone()
    return res[1] if res else ''

# Delete duplicate values and insert new values
def set_last_after(conn, table_name, after):
    c = conn.cursor()
    c.execute('REPLACE INTO state (table_name, after) VALUES (?, ?)', (table_name, after))
    conn.commit()

# Parse search results from the json file
def parse(subreddit, keyword, sort_type, after='', conn=None):
    
    # Set up dynamic URL with UTM params to search
    url_template = 'https://www.reddit.com/r/{}/search.json?q={}&sort={}&restrict_sr=on&t=year{}'

    headers = {
        'User-Agent': 'Mozilla'
    }

    params = f'&after={after}' if after else ''

    url = url_template.format(subreddit, keyword, sort_type, params)
    while True:
        try:
            response = requests.get(url, headers=headers,
                                    proxies={'http': PROXY, 'https': PROXY})
            break
        except:
            pass

    if response.ok:
        c = conn.cursor()
        data = response.json()['data']
        
        # Parse and save the data
        table_name = f'{keyword}_{subreddit}_{sort_type}'
        for post in data['children']:
            pdata = post['data']
            post_id = pdata['id']
            title = pdata['title']
            score = pdata['score']
            author = pdata['author']
            date = pdata['created_utc']
            url = pdata.get('url_overridden_by_dest')
            print(f'{post_id} ({score}) {title}')
            c.execute(f'INSERT OR IGNORE INTO {table_name} VALUES (?,?,?,?,?,?)',
                      (post_id, title, score, author, date, url))
        conn.commit()
        return data['after']
    else:
        print(f'Error {response.status_code}')
        return None


def main():
    # Name of the target subreddits
    subreddits = ['wallstreetbets', 'stocks', 'investing', 'Daytrading', 'StockMarket']
 
    # Define the keywords to search
    keywords = ['aapl', 'apple', 'nflx', 'netflix']
    
    # There are 5 types of sorts in Reddit
    sort_types = ['comments', 'relevance', 'hot', 'new', 'top']
    
    # Create a single connection to the database
    conn = sqlite3.connect('reddit.db')

    # Set up a for loop
    for keyword in keywords:
        for subreddit in subreddits:
            for sort_type in sort_types:
                table_name = f'{keyword}_{subreddit}_{sort_type}'
                create_tables(conn, keyword, subreddit, sort_type)
                after = get_last_after(conn, table_name)
                try:
                    while True:
                        after = parse(subreddit, keyword, sort_type, after, conn)
                        if not after:
                            break
                        set_last_after(conn, table_name, after)
                except KeyboardInterrupt:
                    print('Exiting...')
                print(f'Retrieved all posts about {keyword} in {subreddit} sorted by {sort_type}')

    # Close the database connection
    conn.close()

if __name__ == '__main__':
    main()

import sqlite3
import pandas as pd
# Function to concatenate tables based on keywords and export to CSV
def concatenate_and_export(database, keywords, output_file):
    # Connect to the database
    conn = sqlite3.connect(database)
    c = conn.cursor()
    
    # Get the list of tables
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [table[0] for table in c.fetchall()]
    
    # Filter tables based on keywords
    keyword_tables = [table for table in tables if any(keyword in table for keyword in keywords)]
    
    # Concatenate the data from the selected tables
    df_list = []
    for table in keyword_tables:
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        df_list.append(df)
    
    if df_list:
        concatenated_df = pd.concat(df_list)
    
        # Sort by date and title
        concatenated_df.sort_values(by=['date', 'title'], inplace=True)
        
        # Drop duplicates based on date and title
        concatenated_df.drop_duplicates(subset=['title'], keep='last', inplace=True)
        
        # Export to CSV
        concatenated_df.to_csv(output_file, index=False)
    
    # Close the connection
    conn.close()

# Concatenate and export tables for Apple
concatenate_and_export('reddit.db', ['apple', 'aapl'], 'aapl-subreddits.csv')

# Concatenate and export tables for Netflix
concatenate_and_export('reddit.db', ['netflix', 'nflx'], 'nflx-subreddits.csv')

print("Data exported successfully.")
