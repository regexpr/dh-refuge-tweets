import langid, tweepy, codecs, csv, os, re, sys, time
from datetime import date, datetime
import twitter_credentials
sys.setdefaultencoding('UTF-8')

def getFullTweet(status):
    # case of extended retweet
    try: 
        fullTweet = status.retweeted_status.extended_tweet['full_text']
    except: 
        # case of usual retweet
        try: 
            fullTweet = status.retweeted_status.text
        except:
            # case of extended tweet
            try: 
                fullTweet = status.extended_tweet['full_text']
            except:
                # case of usual tweet
                try: 
                    fullTweet = status.text
                except:
                    # case of something else (change of Twitter's API or an unexpected use case)
                    fullTweet = ''
    return fullTweet

class CustomStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        global old_date
        global writer
        global outfile
        
        new_date = date.today()
        if not new_date == old_date:
            try:
                outfile.close()
            except NameError:
                pass
            outfile = codecs.open("tweets-" + str(new_date) + ".tsv", "ab", "utf-8")
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC, lineterminator='\n', delimiter='\t')
            old_date = new_date
        # count tweets
        numberfile = open("numTweets.txt", "r")
        allNum, gerNum = numberfile.read().split(" ")
        numberfile.close()
        allNum = int(allNum) + 1
        otext = getFullTweet(status)
        lang = langid.classify(otext)[0]
        if lang == "de":
            gerNum = int(gerNum) + 1
            writer.writerow((
                status.created_at,
                status.id,
                otext.replace('\n', ' '),
                status.in_reply_to_status_id,
                status.in_reply_to_user_id,
                status.in_reply_to_screen_name,
                status.user.id,
                status.user.name,
                status.user.location,
                ' '.join(status.user.description.split()) if status.user.description else "",
                status.user.protected,
                status.user.verified,
                status.user.followers_count,
                status.user.friends_count,
                status.user.listed_count,
                status.user.favourites_count,
                status.user.geo_enabled,
                status.geo,
                status.coordinates,
                status.place
            ))
        numberfile = open("numTweets.txt", "w")
        numberfile.write(str(allNum) + " " + str(gerNum))
        numberfile.truncate()
        numberfile.close()

def main():
    global writer, logfile, old_date, outfile
    logfile = open('twython.log', 'a')
    auth = tweepy.OAuthHandler(twitter_credentials.consumer_key, twitter_credentials.consumer_secret)
    auth.set_access_token(twitter_credentials.access_key, twitter_credentials.access_secret)
    api = tweepy.API(auth)
    localtime = time.asctime(time.localtime(time.time()))
    logfile.write(localtime + " Tracking terms from twython-keywords.txt\nStarting stream \n")
    # longer timeout to keep SSL connection open even when few tweets are coming in
    stream = tweepy.streaming.Stream(auth, CustomStreamListener(), tweet_mode='extended', timeout=100.0)
    terms = [line.strip() for line in open('twython-keywords.txt')]
    old_date = date.today()
    outfile = codecs.open("tweets-" + str(old_date) + ".tsv", "ab", "utf-8")
    writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC, lineterminator='\n', delimiter='\t')
    stream.filter(track=terms)