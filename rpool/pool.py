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

'''
Generic class Pool<R> implements "smart" resource pool functionality
for an abstract class R which should extend class AwarePool.Resource.
<p>
This implementation features:
<ul>
<li>    On-demand resource opening;
<li> Resource expiration (close) based on:
   <ul>
   <li> Max idle time
   <li> Max open time
   <li> Max usage count
   </ul>
<li> Arbitrary interval stats that report the following metrics:
   <ul>
   <li> Number of resources taken from the pool (total and last interval)
   <li> Number of resources put back (returned) into the pool (total and last interval)
   <li> Number of resources closed (expired) (total and last interval)
   <li> Number of resources opened on-demand (total and last interval)
   <li> The rates (per second) for the four metrics above
   <li> Resource pool utilization in avg per last interval in percentage and number of resources.
   <li> Resource pool load which is equal to utilization + wait (in percentage and number of resources).
        Note that there might be low utilization/high load situations, for example, when 'open' is slow.
   <li> Current number of resources 'in flight'
   </ul>
</ul>
<p>

@author ikh software, inc.

public class AwarePool<R extends AwarePool.Resource> {

    public static class Resource {

        public Resource() {
        }

        public void resourceOpen() throws AwareResourceException {
            _lastOpened = System.currentTimeMillis();
            _usageCount = 0;
            _open = true;
        }

        public void resourceClose() {
            _open = false;
        }

        public boolean resourceIsOpen() {
            return _open;
        }


        public long resourceGetLastOpened() {
            return _lastOpened;
        }

        public long resourceGetLastUsed() {
            return _lastUsed;
        }

        public void resourceSetLastUsed() {
            _lastUsed = System.currentTimeMillis();
        }

        public long resourceGetUsageCount() {
            return _usageCount;
        }

        public void resourceIncUsageCount() {
            _usageCount++;
        }

        private boolean _open = false;
        private long _lastOpened = 0;
        private long _lastUsed = 0;
        private long _usageCount = 0;
    }

    /**
     * Constructs a new instance of AwarePool&lt;R&gt; of capacity 'capacity'. R must be a class extends class AwarePool.Resource.
     * The pool does not construct resources on its own. After construction, there are no resources in the pool.
     * The 'capacity' number of resources must be 'put' into the pool after its construction.
     * After this initialization, the clients that use the pool must be 'good citizens', i.e. 'take' must precede corresponding 'put', and there
     * must be exactly one 'put' per each 'take', and they should put back resources in consistent state, open or closed.
     *
     * @param capacity            Pool capacity
     * @param maxIdleTime         The resource will be closed if idling in open state for more than this interval (milliseconds).
     * @param maxOpenTime         The resource will be closed if it was open for more than this interval (milliseconds).
     * @param maxUsageCount       The resource will be closed if it was 'taken out' more than this number of times.
     * @param expCheckInterval    How frequently the pool checks resource expiration conditions (milliseconds). This parameter affects the resources.
     *                            that sit idle in the pool; the in-flight resources will be checked at the time they return to the pool).
     * @param statsInterval       How frequently the pool reports pool statistics to stderr (milliseconds).
     * @param takeTimeOut          Take time out in milliseconds. 0 is interpreted as an infinite timeout.
     */
    public AwarePool(int capacity, long maxIdleTime, long maxOpenTime,
            long maxUsageCount, long expCheckInterval, long statsInterval, long takeTimeOut, AwareLogger logger) {
        _startTime = System.currentTimeMillis();
        _capacity = capacity;
        _maxIdleTime = maxIdleTime;
        _maxOpenTime = maxOpenTime;
        _maxUsageCount = maxUsageCount;
        _expCheckInterval = expCheckInterval;
        _statsInterval = statsInterval;
        _takeTimeOut = takeTimeOut;
        _logger = logger;

        _pool = new PriorityBlockingQueue<R>(_capacity, new Comparator<R>() {
            public int compare(R a, R b) {
                long ta = a.resourceGetLastOpened();
                long tb = b.resourceGetLastOpened();
                return -(ta < tb ? -1 : (ta > tb ? 1 : 0));
            }
        });

        _statsThread = new StatsThread();
        _expirationThread = new ExpirationThread();
        _statsThread.start();
        _expirationThread.start();
    }

    /**
     * Takes resource R out of the pool. Waits indefinitely if there are no resources in the pool. Opens resource 'on-demand' id the resource was closed.
     *
     * @return  Resource R.
     */
    public R take() throws AwareResourceException {

        synchronized (_statsThread) {
            _statsThread._cWaiting++;
            _statsThread._utilTimeAddWaiting -= (System.currentTimeMillis() - _statsThread._pCheckTime);
        }

        R r;
        while (true) {
            try {
                if (_takeTimeOut==0) {
                    r = _pool.take();
                } else {
                    r = _pool.poll(_takeTimeOut,TimeUnit.MILLISECONDS);
                    if (r==null) {
                        throw new AwareResourceException("Pool resource take() time out.");
                    }
                }
                break;
            } catch (InterruptedException e) {
            }
        }

        if (!r.resourceIsOpen()) {
            try {
                r.resourceOpen();
            } catch (AwareResourceException e) {
                while (true) {
                    try {
                        _pool.put(r);
                        break;
                    } catch (InterruptedException e2) {
                    }
                }
                synchronized (_statsThread) {
                    long now = System.currentTimeMillis();
                    _statsThread._cWaiting--;
                    _statsThread._utilTimeAddWaiting += (now - _statsThread._pCheckTime);
                }
                throw e;
            }
            synchronized (_statsThread) {
                _statsThread._cOpened++;
            }
        }
        r.resourceIncUsageCount();

        synchronized (_statsThread) {
            long now = System.currentTimeMillis();
            _statsThread._cWaiting--;
            _statsThread._utilTimeAddWaiting += (now - _statsThread._pCheckTime);
            _statsThread._cTaken++;
            _statsThread._utilTimeAdd += (now - _statsThread._pCheckTime);
        }

        return r;
    }

    /**
     * Puts resource R back into the pool. Closes the resource if resource usage count or timestamps meet expiration condition.
     */
    public void put(R r) throws AwareResourceException {
        r.resourceSetLastUsed();
        expire(r);
        while (true) {
            try {
                _pool.put(r);
                break;
            } catch (InterruptedException e) {
            }
        }
        synchronized (_statsThread) {
            _statsThread._utilTimeAdd -= (System.currentTimeMillis() - _statsThread._pCheckTime);
            _statsThread._cPut++;
        }
    }

    /**
     * Returns pool capacity.
     *
     * @return Pool capacity.
     */
    public int capacity() {
        return _capacity;
    }

    private void expire(R r) throws AwareResourceException {
        long now = System.currentTimeMillis();
        if (r.resourceIsOpen()) {
            if (_maxIdleTime > 0
                    && now - r.resourceGetLastUsed() > _maxIdleTime) {
                _logger.info("recycling resource " + r
                        + ", it was idling for "
                        + (now - r.resourceGetLastUsed()));
                r.resourceClose();
                synchronized (_statsThread) {
                    _statsThread._cClosed++;
                }
            } else if (_maxOpenTime > 0
                    && now - r.resourceGetLastOpened() > _maxOpenTime) {
                _logger.info("recycling resource " + r
                        + ", it was open for "
                        + (now - r.resourceGetLastOpened()));
                r.resourceClose();
                synchronized (_statsThread) {
                    _statsThread._cClosed++;
                }
            } else if (_maxUsageCount > 0
                    && r.resourceGetUsageCount() >= _maxUsageCount) {
                _logger.info("recycling resource " + r + ", it was used "
                        + r.resourceGetUsageCount() + " times");
                r.resourceClose();
                synchronized (_statsThread) {
                    _statsThread._cClosed++;
                }
            }
        }
    }

    private class ExpirationThread extends Thread {

        public void run() {
            while (true) {
                LinkedList<R> rs = new LinkedList<R>();
                _pool.drainTo(rs);
                for (R r : rs) {
                    try {
                       expire(r);
                    } catch (AwareResourceException e) {
                       e.printStackTrace();
                    }
                    while (true) {
                        try {
                            _pool.put(r);
                            break;
                        } catch (InterruptedException e) {
                        }
                    }
                }
                try {
                    Thread.sleep(_expCheckInterval);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    private class StatsThread extends Thread {
        public StatsThread() {
            _pCheckTime = System.currentTimeMillis();
        }

        public void run() {
            while (true) {
                long now = System.currentTimeMillis();
                long elapsed;
                long cOpened, nOpened;
                long cClosed, nClosed;
                long cTaken, nTaken;
                long cWaiting, nWaiting;
                long cPut, nPut;
                long nIdle;
                long utilTimeAdd, utilTimeAddWaiting;
                synchronized (this) {
                    elapsed = now - _pCheckTime;
                    _pCheckTime = now;
                    utilTimeAdd = _utilTimeAdd;
                    _utilTimeAdd = 0;
                    utilTimeAddWaiting = _utilTimeAddWaiting;
                    _utilTimeAddWaiting = 0;
                    cOpened = _cOpened;
                    cClosed = _cClosed;
                    cTaken = _cTaken;
                    cWaiting = _cWaiting;
                    cPut = _cPut;
                    nOpened = _cOpened - _pOpened;
                    nClosed = _cClosed - _pClosed;
                    nTaken = _cTaken - _pTaken;
                    nWaiting = _cWaiting - _pWaiting;
                    nPut = _cPut - _pPut;
                    _pOpened = _cOpened;
                    _pClosed = _cClosed;
                    _pTaken = _cTaken;
                    _pWaiting = _cWaiting;
                    _pPut = _cPut;
                    nIdle = _pool.size();
                }
                double rOpened = nOpened * 1000.0 / elapsed;
                double rClosed = nClosed * 1000.0 / elapsed;
                double rTaken = nTaken * 1000.0 / elapsed;
                double rWaiting = nWaiting * 1000.0 / elapsed;
                double rPut = nPut * 1000.0 / elapsed;
                double utilization = capacity()
                        - ((double) (cPut - cTaken) * elapsed + utilTimeAdd)
                        / elapsed;
                double utilizationP = utilization / capacity() * 100.0;
                double load = utilization
                        + ((double) cWaiting * elapsed + utilTimeAddWaiting)
                        / elapsed;
                double loadP = load / capacity() * 100.0;
                //Date logdate = new Date();
                DecimalFormat df = new DecimalFormat("0.00");
                StringBuffer fElapsedSec = df.format((double) elapsed / 1000.0,
                        new StringBuffer(), new FieldPosition(0));
                StringBuffer fLoad = df.format(load, new StringBuffer(),
                        new FieldPosition(0));
                StringBuffer fLoadP = df.format(loadP, new StringBuffer(),
                        new FieldPosition(0));
                StringBuffer fUtilization = df.format(utilization,
                        new StringBuffer(), new FieldPosition(0));
                StringBuffer fUtilizationP = df.format(utilizationP,
                        new StringBuffer(), new FieldPosition(0));
                StringBuffer frOpened = df.format(rOpened, new StringBuffer(),
                        new FieldPosition(0));
                StringBuffer frClosed = df.format(rClosed, new StringBuffer(),
                        new FieldPosition(0));
                StringBuffer frTaken = df.format(rTaken, new StringBuffer(),
                        new FieldPosition(0));
                StringBuffer frWaiting = df.format(rWaiting,
                        new StringBuffer(), new FieldPosition(0));
                StringBuffer frPut = df.format(rPut, new StringBuffer(),
                        new FieldPosition(0));
                _logger.info("pool stats: uptime: " + (now-_startTime) + "ms, elapsed: "
                        + elapsed + "ms, capacity: " + capacity()
                        + ", utilization: " + fUtilizationP + "%/"
                        + fUtilization + ", load: " + fLoadP + "%/" + fLoad
                        + ", in flight: " + (capacity() - nIdle)
                        + "\n  total/last/rate:" + " open: " + cOpened + "/"
                        + nOpened + "/" + frOpened + ", closed: " + cClosed
                        + "/" + nClosed + "/" + frClosed + ", taken: " + cTaken
                        + "/" + nTaken + "/" + frTaken + ", waiting: "
                        + cWaiting + "/" + nWaiting + "/" + frWaiting
                        + ", put: " + cPut + "/" + nPut + "/" + frPut + " ["
                        + (cPut - cTaken) + "/" + (nPut - nTaken) + "]\n");
                try {
                    Thread.sleep(_statsInterval);
                } catch (InterruptedException e) {
                }
            }
        }

        private long _cOpened = 0;
        private long _cClosed = 0;
        private long _cTaken = 0;
        private long _cWaiting = 0;
        private long _cPut = 0;
        private long _pOpened = 0;
        private long _pClosed = 0;
        private long _pTaken = 0;
        private long _pWaiting = 0;
        private long _pPut = 0;
        private long _pCheckTime = 0;
        private long _utilTimeAdd = 0;
        private long _utilTimeAddWaiting = 0;
    }

    private int _capacity;
    private long _maxOpenTime; //milliseconds
    private long _maxIdleTime; //milliseconds
    private long _maxUsageCount; //times
    private BlockingQueue<R> _pool;
    private StatsThread _statsThread;
    private ExpirationThread _expirationThread;
    private long _expCheckInterval;
    private long _statsInterval;
    private long _takeTimeOut;
    private AwareLogger _logger;
    private long _startTime;
}
