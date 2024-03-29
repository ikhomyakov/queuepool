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
# queuepool/smtpcm.py - implements ConnectionManager for SMTP connections
#
import sys
import smtplib
import queuepool.pool as pool

def logg(*args, **kwargs):
   print(*args, file=sys.stderr, **kwargs)

class ConnectionManager(pool.ResourceManager):
   """ ConnectionManager for SMTP connections
   """
   def __init__(self, name, host='', port=0, user=None, password=None, *args, **kwargs):
      super().__init__(name)
      self.host = host
      self.port = port
      self.user = user
      self.password = password

   def __repr__(self):
      return str(dict(host=self.host, port=self.port, user=self.user, password='xxxxxxx', ResourceManager=super().__repr__()))

   def open(self):
      self.resource = smtplib.SMTP(self.host, self.port)
      self.resource.starttls()
      self.resource.login(self.user, self.password)
      super().open()

   def close(self):
      self.resource.quit()
      self.resource = None
      super().close()

   def takeRepair(self):
      super().takeRepair()

   def putRepair(self):
      super().putRepair()

   def sendmail(self, *args, **kwargs):
      self.resource.sendmail(*args, **kwargs)



