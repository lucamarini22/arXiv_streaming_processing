import urllib.request
import time
import feedparser

from kafka import KafkaProducer

# Base api query url
base_url = 'http://export.arxiv.org/api/query?';

# word to search -> put equal to 'a' to search for all papers
# we suppose that every paper contains at least once the letter a
keyword = 'a'
# where to search the word
# all -> means to search for it in the title, or in author names, 
# or in the abstract, or comments, ecc. -> see section 5.1 of 
#https://arxiv.org/help/api/user-manual#query_details
prefix = 'all'

# Search parameters
search_query = prefix + ':' + keyword # search for the keyword in all fields
start = 0                             # start at the first result
total_results = 20                    # want 20 total results
results_per_iteration = 5             # 5 results at a time
wait_time = 3                         # number of seconds to wait beetween calls

# Kafka parameters
topic = 'arXiv'

print('Searching arXiv for %s' % search_query)

i = 0

while(True): 

    print("Results %i - %i" % (i,i+results_per_iteration))
    
    query = 'search_query=%s&start=%i&max_results=%i' % (search_query,
                                                         i,
                                                        results_per_iteration)

    # perform a GET request using the base_url and query
    response = urllib.request.urlopen(base_url+query).read()

    # parse the response using feedparser
    feed = feedparser.parse(response)

    # Run through each entry, and print out information
    for entry in feed.entries:
        print('arxiv-id: %s' % entry.id.split('/abs/')[-1])
        print('Title:  %s' % entry.title)
        # feedparser v4.1 only grabs the first author
        print('First Author:  %s' % entry.author)
    
    # Remember to play nice and sleep a bit before you call
    # the api again!
    print('Sleeping for %i seconds' % wait_time )
    time.sleep(wait_time)

    i += results_per_iteration


    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    producer.send(topic, b'some_message_bytes')


