"""
This file gets the most recent Tweets for one user
"""
import json 
import subprocess
def get_recent_tweets_from_username(username, bearer_token, max_num_tweets=100): 
    """
    Gets the most recent max_num_tweets from the `username`
    """
    #get the Twitter userid from username 
    url = "curl --request GET 'https://api.twitter.com/2/users/by/username/{0}' --header 'Authorization: Bearer {1}'".format(username, bearer_token)
    out = subprocess.run(url, shell=True, capture_output=True)
    userid = json.loads(out.stdout)['data']['id']
    print('userid={0} for username={1}'.format(userid, username))
    
    #get the most recent Tweets 
    # we will not collect retweets or mentions 
    url = "curl -H 'Authorization: Bearer {1}' 'https://api.twitter.com/2/users/{0}/tweets?max_results={2}&exclude=replies%2Cretweets'".format(userid, bearer_token, max_num_tweets)
    out = subprocess.run(url, shell=True, capture_output=True)
    data = json.loads(out.stdout)['data']
    print('from {0} retrieved {1} most recent tweets'.format(username, len(data)))
    return data 

if __name__ == '__main__':
    bearer_token = None #insert this from the Twitter API 

	username = "AOC"
	data = get_recent_tweets_from_username(username, bearer_token, max_num_tweets=100)
	texts = [t['text'] for t in data]
	print(len(texts))

	fout = username+'_last100_tweets.json'
	with open(fout, 'w') as w: 
		json.dump(texts, w)
	print('wrote to : ', fout)