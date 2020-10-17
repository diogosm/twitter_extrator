'''
	precisa de:
		conda
		pip3
		
		snscrape
		pandas
		os
		tweetpy
'''

import os, pandas as pd, tweepy
import datetime
from timeit import default_timer as timer
import time

date_format = '%Y-%m-%d'
start_date = datetime.datetime.strptime("2020-04-06", date_format).date()
end_date = datetime.datetime.strptime("2020-09-01", date_format).date()
## twitter settings ##
consumer_key = "" 
consumer_secret = "" 
access_token = "" 
access_token_secret = ""
## end twitter settings ##
lista_keywords = ['cloroquina', 
	'Hidroxicloroquina',
	'Transmissão assintommatica'#,
	#'Saúde'
	]

def contaTempo(start_time):
	print('Tempo de exec: ', str(datetime.timedelta(seconds=timer()-start_time)))


def processaArq(arq):
	tweet_url = pd.read_csv(arq, index_col= None, header = None, names = ["links"])
	print("------------- Num de links coletados = ", len(tweet_url), flush=True)
	
	## pego o id do tweet
	af = lambda x: x["links"].split("/")[-1]
	tweet_url['id'] = tweet_url.apply(af, axis=1)
	
	
def twitterQuery(start_date, end_date):
	print("------------- buscando tweets -------------")
	print('------------- Intervalo: (', start_date, ', ', end_date, ')')
	
	## usa o snscrape pra buscar os tweets no range de data
	for elem in lista_keywords:
		query = "snscrape twitter-search \"'{}' since:{} until:{} lang:pt-br exclude:retweets\" > out-{}_{}_{}.txt".format(
				elem, str(start_date), str(end_date), elem.replace(' ', '_'), str(start_date), str(end_date)
			)
			
		start_time = timer()
		print(f'------------- Executando query snscrape ({query})...')
		os.system(query)
		contaTempo(start_time)
		
		processaArq("out-{}_{}_{}.txt".format(
			elem.replace(' ', '_'), str(start_date), str(end_date)
		))
		time.sleep(60) ## dorme 60s

	
## realiza busca a cada 5 dias
def busca():
	global start_date, end_date
	step = datetime.timedelta(days=5)
	
	current_date = start_date
	current_date += step
	while current_date < end_date:
		twitterQuery(start_date, current_date.strftime(date_format))
		start_date = current_date + datetime.timedelta(days=1)
		current_date += step
		time.sleep(30) ## dorme 30s
		
	twitterQuery(start_date, end_date)

	
if __name__ == "__main__":
	
	busca()

	'''
	print("buscando tweets")
	##os.system("snscrape twitter-search \"#Sherlock since:2020-01-01 until:2020-01-15\" > sherlock_tweets.txt")
	
	tweet_url = pd.read_csv("sherlock_tweets.txt", index_col= None, header = None, names = ["links"])
	print("Result size = ", len(tweet_url))
	
	tweet_url.head()
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret) 
	auth.set_access_token(access_token, access_token_secret)
	api = tweepy.API(auth)

	
	
	af = lambda x: x["links"].split("/")[-1]
	tweet_url['id'] = tweet_url.apply(af, axis=1)
	ids = tweet_url['id'].tolist()
	total_count = len(ids)
	chunks = (total_count - 1) // 50 + 1
	
	for i in range(chunks):
        batch = ids[i*50:(i+1)*50]
        result = fetch_tw(batch)
	'''
