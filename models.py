from mongokit import Document
import datetime


class Talk(Document):
    structure = {
        'topic': basestring,
        'when': datetime.datetime,
        'duration': float,
        'tags': [basestring],
    }
    required_fields = ['topic', 'when']
