from sns import SNSService
import random
import time
import os


sns_service = SNSService()

topics = sns_service.list_topics()

topics_map = {
    topic['TopicArn'].split(":")[-1]: topic['TopicArn'] for topic in topics['Topics'] if topic['TopicArn'].split(":")[-1].startswith('Escript')
}

topic_keys = list(topics_map.keys())


sentences = []

subject_prefixes = [
    'Big',
    'Important',
    'Super',
    'Intelligent',
    'Unknown',
    'Suspicious',
    'Top Secret'
]
subject_suffixes = [
    'Data',
    'Error',
    'Alert',
    'Data',
    'Message',
    'Event',
    'Stream'
]

for file_path in os.listdir('notes'):
    full_path = os.path.join('notes', file_path)
    if full_path.endswith('.txt'):
        with open(full_path) as fp:
            lines = fp.readlines()
        sentences += lines

for _ in range(100):
    try:
        rand_index = random.randint(0, len(topic_keys) - 1)
        rand_time = random.randint(0, 5)
        rand_topic = topic_keys[rand_index]
        rand_arn = topics_map[rand_topic]
        rand_message_index = random.randint(0, len(sentences) - 1)
        rand_prefix_index = random.randint(0, len(subject_prefixes) - 1)
        rand_suffix_index = random.randint(0, len(subject_suffixes) - 1)
        rand_message = sentences[rand_message_index]
        rand_subject = "{} {}".format(
            subject_prefixes[rand_prefix_index],
            subject_suffixes[rand_suffix_index]
        )
        print('*'*80)
        print(rand_topic)
        print(rand_message)
        print(rand_subject)
        sns_service.publish(
            rand_arn,
            rand_message,
            subject=rand_subject
        )

        time.sleep(rand_time)
    except Exception as e:
        print(str(e))
        pass
