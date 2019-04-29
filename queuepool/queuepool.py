'''
Copyright (c) 2019 ikh software, inc. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
'''

from datetime import datetime

'''
Generic class Pool<R> implements "smart" resource pool for an abstract class R which should extend class Resource and implement open() and close()
This implementation features:
* On-demand resource opening
* Resource recycling (close) based on:
   * Max idle time
   * Max open time
   * Max usage count
'''

class Resource:
   def __init__(self):
      self._isOpen = False
      self._lastOpened = None
      self._lastUsed = None
      self._usageCount = None
      
   def _open(self):
      self._lastOpened = datetime.now()
      self._usageCount = 0
      self._isOpen = True

   def _close(self):
      self._isOpen = False


class Pool:
   '''
   Constructs a new instance of Pool<R> of capacity 'capacity'. R must be a class that extends class Resource.
   The pool does not construct resources on its own. After initialization, there are no resources in the pool.
   The 'capacity' number of resources must be 'put' into the pool after its initialization.
   After this initialization, the clients that use the pool must be 'good citizens', i.e. 'take' must precede corresponding 'put', and there
   must be exactly one 'put' per each 'take', and they should put back resources in consistent state, open or closed.
   @param capacity            Pool capacity (int)
   @param maxIdleTime         The resource will be closed if idling in open state for more than this interval (milliseconds).
   @param maxOpenTime         The resource will be closed if it was open for more than this interval (milliseconds).
   @param maxUsageCount       The resource will be closed if it was 'taken out' more than this number of times.
   '''
   def __init__(self, capacity, maxIdleTime, maxOpenTime, maxUsageCount) {
        self._capacity = capacity
        self._maxIdleTime = maxIdleTime
        self._maxOpenTime = maxOpenTime
        self._maxUsageCount = maxUsageCount
        self._pool = queue.LifoQueue(self._capacity)

   def get():
      '''
      Takes resource R out of the pool. Waits indefinitely if there are no resources in the pool. Opens resource 'on-demand' if the resource is closed.
      @return  Resource R.
      '''
      r = self._pool.get()
      if not r._isOpen:
         r._open()
      r.usageCount += 1
      return r

   def put(r):
      '''
      Puts resource R back into the pool. Closes the resource if resource usage count or timestamps meet expiration condition.
      '''
      r._lastUsed = datetime.now()
      self._recycle(r)
      self._pool.put(r)

   def _recycle(r):
      t = datetime.now()
      if r._isOpen:
         if self._maxIdleTime is not None and t - r._lastUsed > self._maxIdleTime or
            self._maxOpenTime is not None and t - r._lastOpened > self._maxOpenTime or
            self._maxUsageCount is not None and r._usageCount >= self._maxUsageCount:
               r.close()
