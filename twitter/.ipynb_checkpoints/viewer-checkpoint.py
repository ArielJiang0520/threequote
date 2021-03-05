import pandas as pd
import pickle
from nltk.tokenize import TreebankWordTokenizer
import re
from collections import defaultdict
import datetime

class TweetCollection:
    def __init__(self, id):
        self.id = id
        self.df = self.load_df(id)
        self.user_index = defaultdict(list)
        for index, row in self.df.iterrows():
            self.user_index[row['UserName']].append(index)
        
    def clean_df(self, df):
        df = df.drop(['UserScreenName', 'Emojis', 'Image link'], axis=1)
        df['Reply to'] = ''
        for index, row in df.iterrows():
            # reformat replies
            m = re.match(r'(Replying to \n)(@.+)', row['Text'])
            if m:
                df.at[index, 'Reply to'] = m.group(2)
                df.at[index, 'Text'] = df.at[index, 'Embedded_text']

            # reformat timestamp
            df.at[index, 'Timestamp'] = pd.Timestamp(row['Timestamp'], unit='s')

        df = df.drop(['Embedded_text'], axis=1)

        cols = df.columns.tolist()
        cols = cols[:3] + cols[-1:] + cols[3:-1]
        df = df[cols]

        return df

    
    def load_df(self, dataset: str):
        df = pickle.load(open(f'pickle/{dataset}.p', 'rb'))
        return self.clean_df(df)
    
    @staticmethod
    def search_by_words(df, kywds: list):
        res = []
        for index, row in df.iterrows():
            toks = TreebankWordTokenizer().tokenize(row['Text'])
            in_toks = True
            for word in kywds:
                if word not in toks:
                    in_toks = False
                    break
            if in_toks:
                res.append(index)

        # print(f'found: {len(res)} results.')
        return df.iloc[res]

    @staticmethod
    def search_by_user(df, username: str):
        res = []
        for index, row in df.iterrows():
            if row['UserName'] == username:
                res.append(index)

        # print(f'found: {len(res)} results.')
        return df.iloc[res]

    @staticmethod
    def search_by_exact_match(df, string: str):
        res = []
        for index, row in df.iterrows():
            if string in row['Text'].lower():
                res.append(index)

        # print(f'found: {len(res)} results.')
        return df.iloc[res]

    @staticmethod
    def search_by_conversation(df, user: str, support: str):
        res = []
        for index, row in df.iterrows():
            if user == row['UserName'] \
                or (user == row['UserName'] and support == row['Reply to']) \
                or (support == row['UserName'] and user == row['Reply to']):
                res.append(index)
        
        return df.iloc[res].sort_values(by=['Timestamp'])
                
    @staticmethod
    def search_by_reply(df, reply_to: str):
        res = []
        for index, row in df.iterrows():
            if reply_to == row['Reply to']:
                res.append(index)

        # print(f'found: {len(res)} results.')
        return df.iloc[res]
    

#     @staticmethod
#     def extract_words(df, reply_to: str):
        
        
        
    def reply_thread(self, username: str, verbose=False) -> list:
        res = []
        time_elapsed = 0
        dfu = TweetCollection.search_by_user(self.df, username)
        for index, row in dfu.iterrows():
            reply_to = row['Reply to']
            if reply_to in self.user_index and reply_to != username and len(self.user_index[reply_to]) == 1:
                tweet_idx = self.user_index[reply_to][0]
                if self.df.at[tweet_idx, 'Timestamp'] < row['Timestamp']:
                    res.append((self.df.iloc[tweet_idx], row))
                    time_elapsed += (row['Timestamp'] - self.df.at[tweet_idx, 'Timestamp']).total_seconds()
                    if verbose:
                        tweet = self.df.at[tweet_idx, 'Text']
                        print(f"""
    {row['Timestamp']} {row['Tweet URL']}
    {username} replied to {reply_to}\'s tweet:
    \n{reply_to}: {tweet}
    \n{username}: {row['Text']}

    Time Elapsed: {row['Timestamp'] - self.df.at[tweet_idx, 'Timestamp']}

    """)
        # print('Average Reply Time: ', time_elapsed / len(res) / 60)
        return str(datetime.timedelta(seconds=time_elapsed / (len(res) + 1))), res