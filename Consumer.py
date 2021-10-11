from kafka import KafkaConsumer
import feedparser
import pickle

topic = 'arXiv'
print('1')
# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(topic,
                         group_id='my-group',
                         bootstrap_servers=['localhost:9092'])
print('2')
for message in consumer:
  print('aaa')
  print("------")
  print(message)
  print("------")
  print(type(message))
  print("_" * 40)

  message_in_bytes = pickle.dumps(message)
  print(type(message_in_bytes))

  # message value and key are raw bytes -- decode if necessary!
  # e.g., for unicode: `message.value.decode('utf-8')`
  '''
  print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                        message.offset, message.key,
                                        message.value))
  '''
  # parse the response using feedparser
  feed = feedparser.parse(message_in_bytes)

  # Run through each entry, and print out information
  for entry in feed.entries:
      print('arxiv-id: %s' % entry.id.split('/abs/')[-1])
      print('Title:  %s' % entry.title)
      # feedparser v4.1 only grabs the first author
      print('First Author:  %s' % entry.author)
      # Lets get all the categories
      all_categories = [t['term'] for t in entry.tags]
      print('All Categories: %s' % (', ').join(all_categories))
      print('_' * 40)
