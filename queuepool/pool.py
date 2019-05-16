"""A multithread-safe resource pool based on synchronized queue 
"""
# Copyright (c) 2002-2019 Aware Software, inc. All rights reserved.
# Copyright (c) 2005-2019 ikh software, inc. All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

#
# queuepool/pool.py - implements Pool and ResourceManager
#
import sys
import queue
from datetime import datetime
import threading
import time

def logg(*args, **kwargs):
   print(*args, file=sys.stderr, **kwargs)

class ResourceManager:
   """Abstract ResourceManager
   """
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
      #logg(f"ResourceManager: opened resource {self!r}")

   def close(self):
      self._isOpen = False
      #logg(f"ResourceManager: closed resource {self!r}")

   def takeRepair(self):
      pass

   def putRepair(self):
      pass

   def __enter__(self):
      return self

   def __exit__(self, exc_type, exc_value, traceback):
      if self._pool.closeOnException and exc_type is not None:
         self.close()
      self._pool.put(self)
      return False

class Pool:
   """Multithread-safe resource pool based on synchronized queue
   """
   def __init__(self, name, capacity, maxIdleTime=300.0, maxOpenTime=300.0, maxUsageCount=1000, closeOnException=True):
      self.name = name
      self.capacity = capacity
      self._maxIdleTime = maxIdleTime
      self._maxOpenTime = maxOpenTime
      self._maxUsageCount = maxUsageCount
      self.closeOnException = closeOnException
      self._queue = queue.LifoQueue(self.capacity)
      self._recyclerThread = None
      #logg(f"Pool: initialized pool {self!r}")

   def __str__(self):
      return self.name

   def __repr__(self):
      return str(dict(name=self.name, capacity=self.capacity, _maxIdleTime=self._maxIdleTime, _maxOpenTime=self._maxOpenTime, _maxUsageCount=self._maxUsageCount, closeOnException=self.closeOnException))

   def take(self):
      t = datetime.now()
      r = self._queue.get()
      r._pool = self
      self._recycle(r)
      r.takeRepair()
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
      r.putRepair()
      self._queue.put(r)
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

   def recycle(self):
      done = False
      rs = []
      opened0 = 0
      opened1 = 0
      while not done and len(rs) != self.capacity:
         try:
            r = self._queue.get(block=False)
            opened0 += 1 if r._isOpen else 0
            self._recycle(r)
            opened1 += 1 if r._isOpen else 0
            rs.append(r)
         except queue.Empty as e:
            done = True
      for r in reversed(rs):
         self._queue.put(r)
      #logg(f"Pool '{self}': recycled {len(rs)} resources: opened: {opened0} -> {opened1}")
      
   def startRecycler(self, interval=60):
      if self._recyclerThread is None:
         self._recyclerInterval = interval
         self._recyclerThread = threading.Thread(target=self._recyclerLoop, name='queuepool recycler', daemon=True)
         self._recyclerThread.start()
      
   def _recyclerLoop(self):
      while True:
         time.sleep(self._recyclerInterval)
         self.recycle()
   
