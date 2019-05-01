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
# queuepool/psycopg2cm.py - implements ConnectionManager for psycopg2 connections
#
import sys
import psycopg2 as pg
from psycopg2 import extensions as _ext
import queuepool.pool as pool

def logg(*args, **kwargs):
   print(*args, file=sys.stderr, **kwargs)

class ConnectionManager(pool.ResourceManager):
   """ ConnectionManager for psycopg2 connections
   """
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
      if not self.resource.closed:
         self.resource.close()
      self.resource = None
      super().close()

   def repair(self):
      if self._isOpen:
         if not self.resource.closed:
            s = self.resource.info.transaction_status
            if s == _ext.TRANSACTION_STATUS_UNKNOWN:
               # server connection lost
               self.close()
               #logg(f"ConnectionManager: repair: server connection lost: {self}")
            elif s != _ext.TRANSACTION_STATUS_IDLE:
               # connection in error or in transaction (ACTIVE, INTRANS, INERROR)
               self.resource.rollback()
               #logg(f"ConnectionManager: repair: still in transaction: {self}")
            else:
               # regular idle connection
               pass
         else:
            #logg(f"ConnectionManager: repair: connection was closed outside of resource manager: {self}")
            self.close()

   def takeRepair(self):
      super().takeRepair()

   def putRepair(self):
      self.repair()
      super().putRepair()

