queuepool - A multithread-safe resource pool based on synchronized queue
========================================================================

The main problem with psycopg2.pool (https://github.com/psycopg/psycopg2/blob/master/lib/pool.py), for example, is that the pool raises an exception (instead of blocking) when there are no more connections in the pool, and you either have to match the number of connections to the number of workers, or implement retry logic. Also, it doesn't implement connection recycling (on timeout or usage count), and therefore, doesn't fully address issue with stale connections and suited less (scales worse) for large production installations.

This implementation is based on synchronized queue (https://docs.python.org/3/library/queue.html), and thus multithred safe. This is a streamlined port from Java version that was implemented about ten years ago and that has since then been running in heavy production evironment of one of our financial clients.

This implementation features: 

* A pool of generic resources that can be extended for specific resources like psycopg2 connections. Psycopg2 connection pool implementation is provided.
* On-demand lazy resource opening.
* Idle and open timeout recycling. Requires user code to execute `pool.recycle()` method periodically (or start recycler thread by `pool.startRecycler()`), for example, once a minute. If this method isn’t executed periodically, then the recycling is performed only when the resource are either taken or returned back to the pool, and therefore, the pool can accumulate a number of idle connections that exceed the idle or open timeouts.
* Usage count recycling.
* Recycling on exception.
* Recycling on a resource status.
* Context manager allows to use the pool with "with" context manager so that the resources could be returned safely to the pool.
* LIFO queue helps the pool keep number of open resources to the minimum. 

This pool can be utilized successfully in large production installations as it tries to keep the number of open resources to the minimum, yet providing sufficient number of “hot” (open) resources to avoid open/close cost.

License
-------

OSI Approved 3 clause BSD License

Prerequisites
-------------

* Python 3.7+ (with queue)
* For psycopg2 connections: psycopg2 2.8.2+

Installation
------------

If prerequisites are met, you can install `queuepool` like any other Python package, using pip to download it from PyPI:

    $ pip install queuepool

or using `setup.py` if you have downloaded the source package locally:

    $ python setup.py build
    $ sudo python setup.py install
