import urllib.request
import time
import feedparser

from kafka import KafkaProducer

import json
import cate_map
# Base api query url
base_url = 'http://export.arxiv.org/api/query?';

# word to search -> put equal to 'a' to search for all papers
# we suppose that every paper contains at least once the word a
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

cate_dict = cate_map.cate_dict

while(True): 

    print("Produced papers between %i and %i" % (i,i+results_per_iteration))
    
    query = 'search_query=%s&start=%i&max_results=%i' % (search_query,
                                                         i,
                                                        results_per_iteration)

    # perform a GET request using the base_url and query
    response = urllib.request.urlopen(base_url+query).read()

    # uncomment to check that the produced papers info correspond to the consumed ones
    
    # parse the response using feedparser
    feed = feedparser.parse(response)
    print(type(response))
    #print(response)

    # Run through each entry, and print out information
    for entry in feed.entries:
        # print(entry)
        arxiv_id = entry.id.split('/abs/')[-1]
        print('arxiv-id: %s' % arxiv_id)

        title = entry.title
        print('Title:  %s' % title)

        isVersionOne = entry.updated == entry.published
        print('Is first version:  %i' % isVersionOne)

        published_year, published_month, published_day = list(map(int, entry.published.split('T')[0].split('-')))
        print('Published year, month, date:  %i %i %i' % (published_year, published_month, published_day))

        # feedparser v4.1 only grabs the first author
        first_author = entry.author
        print('First Author:  %s' % first_author)
        
        try:
            pageNum = int(entry.arxiv_comment.split('pages')[0])
        except:
            pageNum = -1
        print('Number of pages:  %i' % pageNum)

        # figNum = int(entry.arxiv_comment.split('pages,')[1].split('figure')[0])
        # print('Number of pages:  %i' % pageNum)

        # get all the categories
        all_categories = [t['term'] for t in entry.tags]
        # all_categories = (', ').join(all_categories)
        print('All Categories: %s' % (', ').join(all_categories))

        
        # main category best guess
        try:
            main_cate_guess = all_categories[0].split('.')[0]
            if cate_dict.get(main_cate_guess) is not None:
                human_readable_main_cate_guess = cate_dict.get(main_cate_guess)

            human_readable_cate = []
            for idx, cat in enumerate(all_categories):
                # exclude categories that starts with a digit
                if not cat[0].isdigit():
                    if cate_dict.get(all_categories[idx]) is not None:
                        human_readable_cate.append(cate_dict.get(all_categories[idx]))
            

        except:
            main_cate_guess = "Other"
            human_readable_cate = "Other"
        print('Main category: %s' % main_cate_guess)
        print('Human readable subcategories: %s' % human_readable_cate)
        print('_' * 40)
    i += results_per_iteration


    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    # produce json messages
    producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
    producer.send(topic, 
        {
        'key': arxiv_id, 
        'title': title,
        'isVersionOne': isVersionOne,
        'published_year': published_year,
        'published_month': published_month,
        'published_day': published_day,
        'first_author': first_author,
        'page_num': pageNum,
        'categories': all_categories,
        'main_category': main_cate_guess,
        'human_readable_categories': human_readable_cate,
        'human_readable_main_category': human_readable_main_cate_guess
        }
    )

    # Remember to play nice and sleep a bit before you call
    # the api again!
    # Comment these 2 lines to get more than 1 row (= results_per_iteration papers) 
    # in a batch in the Consumer
    print('Sleeping for %i seconds' % wait_time )
    time.sleep(wait_time)

