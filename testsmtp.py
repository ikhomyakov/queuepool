import queuepool
import queuepool.smtpcm as smtpcm


name = 'smtp'
pool = queuepool.Pool(name=name, capacity=10, maxIdleTime=60, maxOpenTime=600, maxUsageCount=1000, closeOnException=True)
for i in range(pool.capacity):
   pool.put(smtpcm.ConnectionManager(name=name+'-'+str(i), host='smtp.gmail.com', port=587, user="test@gmail.com", password="test"))
pool.startRecycler(5)

with pool.take() as cm:
   cm.sendmail(
      from_addr="test@gmail.com",
      to_addrs="test2@gmail.com",
      msg="Subject: test 3\n\nFollow the link:\nhttps://iridl.ldeo.columbia.edu/\n"
   )
