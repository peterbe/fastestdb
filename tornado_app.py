#!/usr/bin/env python
#-*- coding: iso-8859-1 -*

import uuid
import time
import random
import datetime
import os
import re
import json
import collections
from pprint import pprint
from glob import glob
from functools import partial
from tornado import gen
from tornado import web
from tornado.options import define, options
import tornado.ioloop

try:
    import redis.client as redis_client
except ImportError:
    redis_client = None

try:
    import psycopg2
except ImportError:
    try:
        from psycopg2cffi import compat
        compat.register()
        import psycopg2
    except ImportError:
        psycopg2 = None

try:
    import momoko
except ImportError:
    momoko = None

try:
    import motor
except ImportError:
    motor = None

try:
    import toredis
except ImportError:
    toredis = None

try:
    import mongokit
except ImportError:
    mongokit = None

if mongokit:
    from models import Talk

import pymongo

try:
    from pyelasticsearch import ElasticSearch
except ImportError:
    ElasticSearch = None

define("debug", default=False, help="run in debug mode", type=bool)
define("port", default=8000, help="run on the given port", type=int)

ONE_DAY = datetime.timedelta(days=1)

PG_DSN = (
    'dbname=fastestdb user=peterbe password=test123 '
    'host=localhost port=5432'
)

class MainHandler(web.RequestHandler):
    def get(self):
        self.write("""
        <html>
        <a href=/benchmark?how_many=10>Benchmark</a><br>
        <a href=/aggregate>Aggregate Benchmark</a><br>
        <a href=/prepare>Prepare Benchmark</a>
        </html>
        """)


class BenchmarkHandler(web.RequestHandler):

    @property
    def db(self):
        return self.application.db

    @property
    def momoko_db(self):
        return self.application.momoko_db

    @property
    def redis(self):
        return self.application.redis

    @property
    def toredis(self):
        return self.application.toredis

    @property
    def mongo(self):
        return self.application.mongo_connection['fastestdb']

    @property
    def motor(self):
        return self.application.motor_client['fastestdb_motor']

    @property
    def mongokit(self):
        return self.application.mongokit_connection['fastestdb_mongokit']

    @property
    def es(self):
        return self.application.es

    def _reset_all(self):
        if psycopg2:
            cursor = self.db.cursor()
            cursor.execute("""
                DELETE FROM talks;
                SELECT SETVAL('talks_id_seq', 1, true);
            """)

        if redis_client:
            self.redis.flushall()

        if ElasticSearch:
            self.es.delete_index('talks')

        if pymongo:
            self.application.mongo_connection['fastestdb'].drop_collection('talks')
            self.application.mongo_connection['fastestdb_motor'].drop_collection('talks')
            self.application.mongo_connection['fastestdb_mongokit'].drop_collection('talks')

    def get_all_tests(self):
        TESTS = ()

        if psycopg2:
            TESTS += ('psycopg2',
                      self._create_talks_sql,
                      self._edit_talks_sql,
                      self._delete_talks_sql),
        if redis_client:
            TESTS += ('redis',
                      self._create_talks_redis,
                      self._edit_talks_redis,
                      self._delete_talks_redis),
        if momoko:
            TESTS += ('momoko',
                      self._create_talks_momoko,
                      self._edit_talks_momoko,
                      self._delete_talks_momoko),
        if toredis:
            TESTS += ('toredis',
                      self._create_talks_toredis,
                      self._edit_talks_toredis,
                      self._delete_talks_toredis),
        if pymongo:
            TESTS += ('pymongo',
                      self._create_talks_pymongo,
                      self._edit_talks_pymongo,
                      self._delete_talks_pymongo
                     ),
            TESTS += ('pymongo(safe)',
                      partial(self._create_talks_pymongo, safe=True),
                      partial(self._edit_talks_pymongo, safe=True),
                      partial(self._delete_talks_pymongo, safe=True)
                      ),

        if motor:
            TESTS += ('motor',
                      self._create_talks_motor,
                      self._edit_talks_motor,
                      self._delete_talks_motor
                     ),
            TESTS += ('motor(safe)',
                      partial(self._create_talks_motor, safe=True),
                      partial(self._edit_talks_motor, safe=True),
                      partial(self._delete_talks_motor, safe=True)
                      ),

        if mongokit:
            TESTS += ('mongokit',
                      self._create_talks_mongokit,
                      self._edit_talks_mongokit,
                      self._delete_talks_mongokit
                     ),
            TESTS += ('mongokit(safe)',
                      partial(self._create_talks_mongokit, safe=True),
                      partial(self._edit_talks_mongokit, safe=True),
                      partial(self._delete_talks_mongokit, safe=True)
                      ),

        if ElasticSearch:
            TESTS += ('elasticsearch',
                      self._create_talks_es,
                      self._edit_talks_es,
                      self._delete_talks_es
                     ),

        return TESTS


    @web.asynchronous
    @gen.engine
    def get(self):
        how_many = int(self.get_argument('how_many', 1))
        sleep_time = float(self.get_argument('sleep_time', 0.1))

        labels = self.get_arguments('labels', None)
        ioloop_instance = tornado.ioloop.IOLoop.instance()

        if int(self.get_argument('reset_first', 0)):
            self._reset_all()

        def write(filename, label, timing):
            with open(os.path.join('timings', filename), 'a') as f:
                f.write('%s\t%s\t%s\n' % (label, how_many, timing))

        tests = []
        for label, creator, editor, deletor in self.get_all_tests():
            if labels and label not in labels:
                continue
            log_file = '%s.log' % label.replace('(', '_').replace(')', '')
            test = [label]
            total = 0.0

            t0 = time.time()
            ids = yield gen.Task(creator, how_many)
            t1 = time.time()
            total += t1-t0
            test.append(
                t1 - t0
            )
            write(log_file, 'create', t1 - t0)

            # give it a rest so that the database can internally index things
            if sleep_time:
                yield gen.Task(
                    ioloop_instance.add_timeout,
                    time.time() + sleep_time
                )

            t0 = time.time()
            yield gen.Task(editor, ids)
            t1 = time.time()
            total += t1 - t0
            test.append(
                t1 - t0
            )
            write(log_file, 'edit', t1 - t0)

            # give it a rest so that the database can internally index things
            if sleep_time:
                yield gen.Task(
                    ioloop_instance.add_timeout,
                    time.time() + sleep_time
                )

            t0 = time.time()
            yield gen.Task(deletor, ids)
            t1 = time.time()
            total += t1 - t0
            test.append(
                t1 - t0
            )
            test.append(total)
            write(log_file, 'delete', t1 - t0)

            tests.append([isinstance(x, float) and round(x, 4) or x
                          for x in test])

        self.render('benchmark.html', tests=tests, aggregate=False)

    ##
    ## Momoko SQL
    ##

    @gen.engine
    def _create_talks_momoko(self, how_many, callback):
        ids = set()
        exc = self.momoko_db.execute
        for i in range(how_many):
            topic = _random_topic()
            when = _random_when()
            tags = _random_tags()
            duration = _random_duration()
            cursor = yield momoko.Op(
                exc,
                "select nextval('talks_id_seq');"
            )
            pk, = cursor.fetchone()
            #
            yield momoko.Op(
                exc,
                """
                insert into talks (id, topic, "when", tags, duration) values (
                %s,
                %s,
                %s,
                %s,
                %s
                );
                """,
                (pk, topic, when, tags, duration)
            )
            ids.add(pk)
        callback(ids)

    @gen.engine
    def _edit_talks_momoko(self, ids, callback):
        exc = self.momoko_db.execute
        for pk in ids:
            cursor = yield momoko.Op(
                exc,
                'select topic, duration, "when", tags from talks where id=%s',
                (pk,)
            )
            topic, duration, when, tags = cursor.fetchone()
            yield momoko.Op(
                exc,
                'update talks set '
                'topic = %s, duration = %s, "when" = %s, tags = %s'
                'where id=%s',
                (topic + 'extra',
                 duration + 1.0,
                 when + ONE_DAY,
                 tags + ['extra'],
                 pk)
            )
        callback()

    @gen.engine
    def _delete_talks_momoko(self, ids, callback):
        exc = self.momoko_db.execute
        for pk in ids:
            yield momoko.Op(
                exc,
                'delete from talks where id=%s',
                (pk,)
            )
        callback()

    ##
    ## psycopg2 SQL
    ##

    @gen.engine
    def _create_talks_sql(self, how_many, callback):
        ids = set()
        cursor = self.db.cursor()
        for i in range(how_many):
            topic = _random_topic()
            when = _random_when()
            tags = _random_tags()
            duration = _random_duration()

            cursor.execute(
                "select nextval('talks_id_seq');"
            )
            pk, = cursor.fetchone()
            #
            cursor.execute(
                """
                insert into talks (id, topic, "when", tags, duration) values (
                %s,
                %s,
                %s,
                %s,
                %s
                );
                """,
                (pk, topic, when, tags, duration)
            )
            ids.add(pk)
        cursor.close()
        callback(ids)

    @gen.engine
    def _edit_talks_sql(self, ids, callback):
        cursor = self.db.cursor()
        for pk in ids:
            cursor.execute(
                'select topic, duration, "when", tags from talks where id=%s',
                (pk,)
            )
            topic, duration, when, tags = cursor.fetchone()
            cursor.execute(
                'update talks set '
                'topic = %s, duration = %s, "when" = %s, tags = %s'
                'where id=%s',
                (topic + 'extra',
                 duration + 1.0,
                 when + ONE_DAY,
                 tags + ['extra'],
                 pk)
            )
        cursor.close()
        callback()

    @gen.engine
    def _delete_talks_sql(self, ids, callback):
        cursor = self.db.cursor()
        for pk in ids:
            cursor.execute(
                'delete from talks where id=%s',
                (pk,)
            )
        cursor.close()
        callback()


    ##
    ## pyelasticsearch
    ##

    @gen.engine
    def _create_talks_es(self, how_many, callback):
        ids = set()
        docs = []

        for i in range(how_many):
            document = dict(
                topic=_random_topic(),
                when=_random_when(),
                tags=_random_tags(),
                duration=_random_duration()
            )
            document['when'] = time.mktime(document['when'].timetuple())
            document['id'] = uuid.uuid4().hex
            ids.add(document['id'])
            docs.append(document)
        results = self.es.bulk_index(
            'talks',
            'talk',
            docs
        )
        #ids = [x['create']['_id'] for x in results['items']]
            #r = self.es.index(
            #    'talks',
            #    'talk',
            #    document)
            #ids.add(r['_id'])
        callback(ids)

    @gen.engine
    def _edit_talks_es(self, ids, callback):
        for pk in ids:
            thing = self.es.get('talks', 'talk', pk)
            document = thing['_source']
            document['topic'] += 'extra'
            document['duration'] += 1.0
            when = datetime.datetime.fromtimestamp(document['when'])
            document['when'] = time.mktime((when + ONE_DAY).timetuple())
            document['tags'] += ['extra']
            self.es.index(
                'talks',
                'talk',
                document,
                id=thing['_id']
            )
        callback()

    def _delete_talks_es(self, ids, callback):
        for pk in ids:
            self.es.delete('talks', 'talk', pk)
        callback()

    ##
    ## Redis JSON (blocking)
    ##

    @gen.engine
    def _create_talks_redis(self, how_many, callback):
        ids = set()
        set_ = self.redis.set
        for i in range(how_many):
            topic = _random_topic()
            when = _random_when()
            tags = _random_tags()
            duration = _random_duration()
            pk = uuid.uuid4().hex
            document = {
                'topic': topic,
                'when': time.mktime(when.timetuple()),
                'tags': tags,
                'duration': duration,
            }
            set_(pk, json.dumps(document))
            ids.add(pk)
        callback(ids)

    @gen.engine
    def _edit_talks_redis(self, ids, callback):
        set_ = self.redis.set
        get = self.redis.get
        for pk in ids:
            document = json.loads(get(pk))
            document['topic'] += 'extra'
            document['duration'] += 1.0
            when = datetime.datetime.fromtimestamp(document['when'])
            document['when'] = time.mktime((when + ONE_DAY).timetuple())
            document['tags'] += ['extra']
            set_(pk, json.dumps(document))
        callback()

    @gen.engine
    def _delete_talks_redis(self, ids, callback):
        db = self.redis
        for pk in ids:
            db.delete(pk)
        callback()

    ##
    ## ToRedis (non-blocking)
    ##

    @gen.engine
    def _create_talks_toredis(self, how_many, callback):
        ids = set()
        set_ = self.toredis.set
        for i in range(how_many):
            topic = _random_topic()
            when = _random_when()
            tags = _random_tags()
            duration = _random_duration()
            pk = uuid.uuid4().hex
            document = {
                'topic': topic,
                'when': when.strftime('%Y-%m-%d %H:%M:%S'),
                'tags': tags,
                'duration': duration,
            }
            yield gen.Task(set_, pk, json.dumps(document))
            ids.add(pk)
        callback(ids)

    @gen.engine
    def _edit_talks_toredis(self, ids, callback):
        set_ = self.toredis.set
        get = self.toredis.get
        for pk in ids:
            document = yield gen.Task(get, pk)
            document = json.loads(document)
            document['topic'] += 'extra'
            document['duration'] += 1.0
            when = datetime.datetime.strptime(
                document['when'],
                '%Y-%m-%d %H:%M:%S'
            )
            document['when'] = (when + ONE_DAY).strftime('%Y-%m-%d %H:%M:%S')
            document['tags'] += ['extra']
            yield gen.Task(set_, pk, json.dumps(document))
        callback()

    @gen.engine
    def _delete_talks_toredis(self, ids, callback):
        for pk in ids:
            yield gen.Task(self.toredis.delete, pk)
        callback()

    ##
    ## PyMongo (blocking)
    ##

    @gen.engine
    def _create_talks_pymongo(self, how_many, callback, safe=False):
        ids = set()
        collection = self.mongo.talks
        for i in range(how_many):
            topic = _random_topic()
            when = _random_when()
            tags = _random_tags()
            duration = _random_duration()
            document = {
                'topic': topic,
                'when': when,
                'tags': tags,
                'duration': duration,
            }
            pk = collection.insert(
                document,
                w=safe and 1 or 0
            )
            ids.add(pk)
        callback(ids)

    @gen.engine
    def _edit_talks_pymongo(self, ids, callback, safe=False):
        collection = self.mongo.talks
        for pk in ids:
            document = collection.find_one({'_id': pk})
            document['topic'] += 'extra'
            document['duration'] += 1.0
            document['when'] += ONE_DAY
            document['tags'] += ['extra']
            collection.update(
                {'_id': pk},
                document,
                w=safe and 1 or 0
            )
        callback()

    @gen.engine
    def _delete_talks_pymongo(self, ids, callback, safe=False):
        collection = self.mongo.talks
        for pk in ids:
            collection.remove(
                {'_id': pk},
                w=safe and 1 or 0
            )
        callback()

    ##
    ## Motor (non-blocking mongo)
    ##

    @gen.engine
    def _create_talks_motor(self, how_many, callback, safe=False):
        ids = set()
        collection = self.motor.talks
        for i in range(how_many):
            topic = _random_topic()
            when = _random_when()
            tags = _random_tags()
            duration = _random_duration()
            document = {
                'topic': topic,
                'when': when,
                'tags': tags,
                'duration': duration,
            }
            pk = yield motor.Op(
                collection.insert,
                document,
                w=safe and 1 or 0
            )
            ids.add(pk)
        callback(ids)

    @gen.engine
    def _edit_talks_motor(self, ids, callback, safe=False):
        collection = self.motor.talks
        for pk in ids:
            document = yield motor.Op(
                collection.find_one,
                {'_id': pk}
            )
            assert document, pk
            document['topic'] += 'extra'
            document['duration'] += 1.0
            document['when'] += ONE_DAY
            document['tags'] += ['extra']
            yield motor.Op(
                collection.update,
                {'_id': pk},
                document,
                w=safe and 1 or 0
            )
        callback()

    @gen.engine
    def _delete_talks_motor(self, ids, callback, safe=False):
        collection = self.motor.talks
        for pk in ids:
            yield motor.Op(
                collection.remove,
                {'_id': pk},
                w=safe and 1 or 0
            )
        callback()


    ##
    ## MongoKit (blocking)
    ##

    @gen.engine
    def _create_talks_mongokit(self, how_many, callback, safe=False):
        ids = set()
        collection = self.mongokit.talks
        for i in range(how_many):
            topic = _random_topic()
            when = _random_when()
            tags = _random_tags()
            duration = _random_duration()
            talk = collection.Talk()
            talk['topic'] = topic
            talk['when'] = when
            talk['tags'] = tags
            talk['duration'] = duration
            talk.save(safe=safe)
            ids.add(talk['_id'])
        callback(ids)

    @gen.engine
    def _edit_talks_mongokit(self, ids, callback, safe=False):
        collection = self.mongokit.talks
        for pk in ids:
            talk = collection.Talk.find_one({'_id': pk})
            talk['topic'] += 'extra'
            talk['duration'] += 1.0
            talk['when'] += ONE_DAY
            talk['tags'] += ['extra']
            talk.save(safe=safe)
        callback()

    @gen.engine
    def _delete_talks_mongokit(self, ids, callback, safe=False):
        collection = self.mongokit.talks
        for pk in ids:
            collection.remove({'_id': pk}, safe=safe)
        callback()


class AggregateBenchmarkHandler(BenchmarkHandler):

    def get(self):
        _rounds = {}
        _counts = collections.defaultdict(int)
        for each in glob('timings/*.log'):
            label = os.path.basename(each).replace('.log', '')
            label = re.sub('_(\w+)', r'(\1)', label)
            if label not in _rounds:
                _rounds[label] = collections.defaultdict(float)
            for line in open(each):
                test, how_many, time_ = line.split()
                how_many = int(how_many)
                time_ = float(time_)
                _rounds[label][test] += time_
                _counts[label] += how_many
        if len(set(_counts.values())) != 1:
            pprint(_counts)
            raise ValueError(
                "Not all tests have run the same amount of counts. "
                "You need to reset the timings."
            )
        operations = _counts.values()[0]

        tests = []
        for label, timings in _rounds.items():
            test = [label]
            test.append(timings['create'])
            test.append(timings['edit'])
            test.append(timings['delete'])
            test.append(sum(timings.values()))
            tests.append([isinstance(x, float) and round(x, 4) or x
                          for x in test])

        self.render(
            'benchmark.html',
            tests=tests,
            operations=operations,
            aggregate=True
        )


class PrepareBenchmarkHandler(BenchmarkHandler):
    def get(self):
        labels = [x[0] for x in self.get_all_tests()]
        self.render('prepare.html', labels=labels)


class LabelsHandler(BenchmarkHandler):
    def get(self):
        labels = [x[0] for x in self.get_all_tests()]
        self.write({'labels': labels})


def _random_topic():
    return random.choice(
        (u'No talks added yet',
         u"I'm working on a branch of django-mongokit that I thought you'd like to know about.",
         u'I want to learn Gaelic.',
         u"I'm well, thank you.",
         u' (Kaw uhn KEU-ra shin KAW-la root uh CHOO-nik mee uhn-royer?)',
         u'Chah beh shin KEU-ra, sheh shin moe CHYEH-luh uh vah EEN-tchuh!',
         u'STUH LUH-oom BRISS-kaht-chun goo MAWR',
         u"Suas Leis a' Ghàidhlig! Up with Gaelic!",
         u"Tha mi ag iarraidh briosgaid!",
        ))

def _random_when():
    return datetime.datetime(random.randint(2000, 2010),
                             random.randint(1, 12),
                             random.randint(1, 28),
                             0, 0, 0)#.replace(tzinfo=utc)

def _random_tags():
    tags = [u'one', u'two', u'three', u'four', u'five', u'six',
            u'seven', u'eight', u'nine', u'ten']
    random.shuffle(tags)
    return tags[:random.randint(0, 3)]

def _random_duration():
    return round(random.random() * 10, 1)


routes = [
    (r"/", MainHandler),
    (r"/benchmark", BenchmarkHandler),
    (r"/aggregate", AggregateBenchmarkHandler),
    (r"/prepare", PrepareBenchmarkHandler),
    (r"/labels", LabelsHandler),
]

if __name__ == "__main__":
    tornado.options.parse_command_line()

    application = web.Application(
        routes,
        debug=options.debug,
        template_path=os.path.join(os.path.dirname(__file__), "templates"),
        static_path=os.path.join(os.path.dirname(__file__), "static"),
    )


    if momoko:
        application.momoko_db = momoko.Pool(
            dsn=PG_DSN,
            size=5,
        )

    if psycopg2:
        application.db = psycopg2.connect(PG_DSN)

    if redis_client:
        application.redis = redis_client.Redis(
            'localhost',
            6379
        )

    if toredis:
        application.toredis = toredis.Client()
        application.toredis.connect(
            'localhost',
            6379
        )

    if pymongo:
        application.mongo_connection = pymongo.MongoClient()
    if motor:
        application.motor_client = motor.MotorClient().open_sync()
    if mongokit:
        application.mongokit_connection = mongokit.Connection()
        application.mongokit_connection.register([Talk])
    if ElasticSearch:
        application.es = ElasticSearch('http://localhost:9200')

    print "Starting tornado on port", options.port
    application.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
