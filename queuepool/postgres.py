import sys
import queue
import psycopg2 as pg
from datetime import datetime

def logg(*args, **kwargs):
   print(*args, file=sys.stderr, **kwargs)

class ResourceManager:

   def __init__(self, name):
      self.name = name
      self.resource = None
      self._pool = None
      self._isOpen = False
      self._lastOpened = None
      self._lastUsed = None
      self._usageCount = None
      #logg(f"ResourceManager: initialized resource {self!r}")

   def __str__(self):
      return self.name

   def __repr__(self):
      return str(dict(name=self.name, _isOpen=self._isOpen, _lastOpened=self._lastOpened, _lastUsed=self._lastUsed, _usageCount=self._usageCount))

   def open(self):
      t = datetime.now()
      self._isOpen = True
      self._lastOpened = t
      self._lastUsed = t
      self._usageCount = 0
      logg(f"ResourceManager: opened resource {self!r}")

   def close(self):
      self._isOpen = False
      logg(f"ResourceManager: closed resource {self!r}")

   def __enter__(self):
      return self.resource

   def __exit__(self, exc_type, exc_value, traceback):
      if self._pool.closeOnException and exc_type is not None:
         self.close()
      self._pool.put(self)
      return False


class ConnectionManager(ResourceManager):

   def __init__(self, name, isolation_level=None, readonly=None, deferrable=None, autocommit=None, *args, **kwargs):
      super().__init__(name)
      self.autocommit = autocommit
      self.isolation_level = isolation_level
      self.readonly = readonly
      self.deferrable = deferrable
      self._args = args
      self._kwargs = kwargs

   def __repr__(self):
      return str(dict(autocommit=self.autocommit, isolation_level=self.isolation_level, readonly=self.readonly, deferrable=self.deferrable, ResourceManager=super().__repr__()))

   def open(self):
      self.resource = pg.connect(*self._args, **self._kwargs)
      self.resource.set_session(isolation_level=self.isolation_level, readonly=self.readonly, deferrable=self.deferrable, autocommit=self.autocommit)
      super().open()

   def close(self):
      self.resource.close()
      self.resource = None
      super().close()



class Pool:

   def __init__(self, name, capacity, maxIdleTime=300.0, maxOpenTime=300.0, maxUsageCount=1000, closeOnException=True):
      self.name = name
      self.capacity = capacity
      self._maxIdleTime = maxIdleTime
      self._maxOpenTime = maxOpenTime
      self._maxUsageCount = maxUsageCount
      self.closeOnException = closeOnException
      self._pool = queue.LifoQueue(self.capacity)
      #logg(f"Pool: initialized pool {self!r}")

   def __str__(self):
      return self.name

   def __repr__(self):
      return str(dict(name=self.name, capacity=self.capacity, _maxIdleTime=self._maxIdleTime, _maxOpenTime=self._maxOpenTime, _maxUsageCount=self._maxUsageCount, closeOnException=self.closeOnException))

   def take(self):
      t = datetime.now()
      r = self._pool.get()
      r._pool = self
      self._recycle(r)
      if not r._isOpen:
         r.open()
      r._usageCount += 1
      r._lastUsed = t
      #logg(f"Pool '{self}': took resource {r!r}")
      return r

   def put(self,r):
      t = datetime.now()
      r._pool = self
      r._lastUsed = t
      self._recycle(r)
      self._pool.put(r)
      #logg(f"Pool '{self}': put resource {r!r}")

   def _recycle(self,r):
      t = datetime.now()
      if r._isOpen:
         isMaxIdle = self._maxIdleTime is not None and (t - r._lastUsed).total_seconds() > self._maxIdleTime
         isMaxOpen = self._maxOpenTime is not None and (t - r._lastOpened).total_seconds() > self._maxOpenTime
         isMaxUsage = self._maxUsageCount is not None and r._usageCount >= self._maxUsageCount
         if isMaxIdle or isMaxOpen or isMaxUsage:
               r.close()
               #logg(f"Pool '{self}': recycled resource {r!r}, {dict(isMaxIdle=isMaxIdle, isMaxOpen=isMaxOpen, isMaxUsage=isMaxUsage)}")


if __name__ == '__main__':
   pool = Pool(name='iridlauth', capacity=10, maxIdleTime=60, maxOpenTime=600, maxUsageCount=1000, closeOnException=True)
   for i in range(pool.capacity):
      pool.put(ConnectionManager(name='iridlauth-'+str(i), autocommit=False, isolation_level=pg.extensions.ISOLATION_LEVEL_SERIALIZABLE, host='localhost', dbname='iridlauth', user='ikh', password='hernya8'))

   # with context manager
   with pool.take() as conn:
      with conn.cursor() as c:
         c.execute("select count(*) from accounts")
         rs = c.fetchall() + [conn.get_backend_pid()]
         logg('Result 1: ', rs)
         #raise Exception('bum!')

   # without context manager
   connmgr = pool.take()
   try:
      conn = connmgr.resource
      with conn.cursor() as c:
         c.execute("select count(*) from accounts")
         rs = c.fetchall() + [conn.get_backend_pid()]
         logg('Result 2: ', rs)
         #raise Exception('bam!')
   except Exception as e:
      if pool.closeOnException:
         connmgr.close()
      raise
   finally:
      pool.put(connmgr)

   # example with thread pool
   import multiprocessing.pool as mpp
   import time
   import random
   def fetch(dbpool, name):
      res = []
      for i in range(10):
         with dbpool.take() as conn:
            with conn: # transaction
               with conn.cursor() as c:
                  completed = False
                  while not completed:
                     try:
                        c.execute("select x from x")
                        #time.sleep(0.01)
                        (x,), = c.fetchall()
                        x = x + 1
                        c.execute("update x set x=%s", (x,))
                        res.append(x)
                        completed = True
                     except pg.errors.SerializationFailure as e:
                        conn.rollback()
                        logg('pg.errors.SerializationFailure: ', name, i, x)
                        pass
                     if not completed:
                        time.sleep(random.random() * 0.01) 
                  #logg(name, i, x)
      return res

   tpool = mpp.ThreadPool(20)
   rs = tpool.starmap(fetch, [(pool, 'task-'+str(x)) for x in range(100)])
   logg(sorted(sum(rs,[])))

