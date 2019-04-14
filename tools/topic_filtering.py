import pandas as pd
import nltk
from nltk.stem.cistem import Cistem

# Initialise
tokenizer = nltk.tokenize.TweetTokenizer(strip_handles=True, reduce_len=True)
stemmer = Cistem()

# Read Dataset
tweets = pd.read_csv('data/all_tweets.tsv', sep='\t', header=None)

# Provide rehashed wordlist that is used to filter tweets by topic
keywords = []
index_topic_tweets = []
inp = open ("../data/topic_wordlist.txt","r")
for line in inp.readlines():
    line = line.replace('\n','')
    keywords.append(line)

for word in keywords:
    word = word.replace('ä', 'ae')
    word = word.replace('ö', 'oe')
    word = word.replace('ü', 'ue')
    word = word.replace('ß', 'ss')
    word = word.lower()
    keyword = stemmer.stem(word)

for index, row in tweets.iterrows():
    # Tokenization
    words = tokenizer.tokenize(row[2])
    # Remove short tokens
    for word in words:
        if len(word)>2:
            word = word.lower()
            word = word.replace("#", "")
            word = word.replace('ä', 'ae')
            word = word.replace('ö', 'oe')
            word = word.replace('ü', 'ue')
            word = word.replace('ß', 'ss')
            word = stemmer.stem(word)
            if word not in nltk.corpus.stopwords.words('german'):
                if word in keywords and index not in index_topic_tweets:
                    index_topic_tweets.append(index)

refugees_tweets = tweets.loc[ index_topic_tweets,:]
refugees_tweets.to_csv("output/refugee_tweets.tsv",index=False,sep="\t")