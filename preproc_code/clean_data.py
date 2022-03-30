"""
This file creates a clean dataset
"""
import pandas as pd 
import numpy as np
from tqdm import tqdm
import subprocess
import json 

def tweet_text_given_id(tweet_id): 
    """
    Calls the Twitter API on the command line 
    
    Returns: 
        Tweet text given the tweet_id 
    """
    url ='curl "https://api.twitter.com/2/tweets/{0}?tweet.fields=text" -H "Authorization: Bearer {1}"'.format(tweet_id, bearer_token)
    out = subprocess.run(url, shell=True, capture_output=True)
    try:
        return json.loads(out.stdout)['data']['text']
    except:
        return None

def go_rehydrate(df_ratings):
	# find rows of the dataset that don't have this bad tweet_id issue 
	# I can't query tweet ids that look like "1.0448E+18"
	good_rows = [i for i, x in enumerate(df_ratings['tweet_id']) if 'E' not in str(x)]
	print(len(good_rows)) 
	df_subset = df_ratings.iloc[good_rows]

	tweet_texts = []
	for row, tweet_id in tqdm(enumerate(df_subset['tweet_id'])): 
	    tweet_id = str(tweet_id).strip('x')
	    text = tweet_text_given_id(tweet_id, bearer_token)
	    tweet_texts. append({'row': row, 'text': text, 'tweet_id': tweet_id})

	# save these so don't have to re-run the Twitter API 
	fout = 'tweets_saved.json'
	with open(fout, 'w') as w: 
	    json.dump(tweet_texts, w)

	df_text = pd.DataFrame(tweet_texts)
	return df_text

def replace_series(col): 
    orig2stan = {
        1 : 1, #True 
        2 : 0, #False 
        3 : None  
    }
    return [orig2stan[y] for y in col]

def main(rehydrate_tweets=False): 
	fname = 'idsRatings16k.csv' #got this from an email from Killian (one of the authors)
	df_ratings = pd.read_csv(fname)
	col_names = ['con'+str(x) for x in range(1, 6)]+['lib'+str(x) for x in range(1, 6)]

	if rehydrate_tweets: 
		df_text = go_rehydrate(df_ratings)
	else: 
		fin = 'tweets_saved.json'
		tweet_texts = json.load(open(fin, 'r'))
		df_text = pd.DataFrame(tweet_texts)
		print('read ', fin, ' len=', len(df_text))

	# drop where we couldn't rehydrate 
	df_text_good = df_text.dropna()

	#clean up the tweet id 
	df2 = df_ratings.copy()
	df2['tweet_id_clean'] = [str(x).strip('x') for x in df_ratings['tweet_id']]
	del df2['tweet_id']

	#join with the original 
	df_full = pd.merge(df_text_good, df2, how='left', left_on='tweet_id', right_on='tweet_id_clean')

	#make into standard 0/1 format 
	df_full = df_full.apply(lambda x : replace_series(x) if x.name in col_names else x)

	# get rid of any nulls 
	df3 = df_full.dropna()
	df3 = df3.astype({c: 'int64' for c in col_names})
	del df3['tweet_id_clean']
	del df3['row']

	#remove the tweet id for the tutorial 
	del df3['tweet_id']

	# # add the labels 
	# ### brady7 -> at least 7 said yes 
	# ### majority vote -> at least 5 said yes 
	# df_mean = df3[col_names].mean(axis=1)
	# df3['label_raw_mean'] = df_mean 
	# df3['label_brady7'] = np.array((df3['label_raw_mean'] >= 0.7).values).astype(int)
	# df3['label_majority'] = np.array((df3['label_raw_mean'] >= 0.5).values).astype(int)

	print('final dataset len=', len(df3))
	fout = '../data/kavanaugh_clean.csv'
	df3.to_csv(fout, index=False)
	print('wrote to ', fout)

if __name__ == '__main__':
	bearer_token = None #insert this from the Twitter API 
	main(rehydrate_tweets=True)