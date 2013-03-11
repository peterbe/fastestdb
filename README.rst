Fastest DB
==========

By Peter Bengtsson, <mail@peterbe.com>


A benchmark to see which database is the fastest at doing a bunch of
operations in Tornado.

About
-----

This little benchmark script runs a simple process:

1. Create X "talks" containing:

  * A unicode string (topic)

  * A datetime instance (when)

  * A floating point number (duration)

  * An array/list (tags)

2. Edit each "talk" one by one by...:

  * Adding some text to the topic

  * Adding a day to the when

  * Adding 0.1 to the duration

  * Adding one extra tag

3. For each ID remove that row/document

It then sums up how long it took each engine to complete these tasks.


Requirements
------------

First of all, you need to have the following databases available on
the default port running on localhost:

* postgresql (I used PG 9.2)

* redis (I used 2.2.8)

* mongodb (I used 2.2.1)

Second you need to install all the python dependencies::

    $ pip install -r requirements.txt

(some versions are pinned, perhaps that's not important)

To set up the postgres table simply load in ``schema.sql``. Like this::

    $ createdb fastestdb
    $ psql fastestdb < schema.sql


How to run
----------

The first thing you need to do is to start the Tornado server::

    $ python tornado_app.py

Note: if you want to hack on the code, add ``--debug`` to that so it
restarts automatically when you edit the file.

Then simply go to::

    http://localhost:8000/benchmark

To change how many iterations simply change the number, e.g::

    http://localhost:8000/benchmark?how_many=1000


Aggregate runs
--------------

All timings are recorded in a directory called ``./timings/`` as
``.log`` files. If you instead go to::

    http://localhost:8000/aggregate

it will sum all timings from multiple runs. This directory is not
reset unless you do so manually.
