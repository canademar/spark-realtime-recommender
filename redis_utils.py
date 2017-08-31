#!/usr/bin/redis
import operator
from collections import Counter
from redis import Redis


class RedisUtils:


    def __init__(self, host, port):
        self.redis = Redis(host, port)

    def userCluster(self, user):       
        return self.redis.hget("clusterAssignment", user).decode("utf8")

    def mostViewedIntCluster(self, cluster):
        clusterKeys = self.redis.keys("cluster_%s*" % cluster)
        clusterKeys = list(map(lambda x: x.decode("utf8").split("_")[3], clusterKeys))
        counted = Counter(clusterKeys)
        print(counted)
        sortedItems = sorted(counted, key=counted.get, reverse=True)
        print(sortedItems)
        return sortedItems[0:10]
        

if __name__=="__main__":
    print("Hi")
    r = RedisUtils("localhost", 6379)
    user = 3
    print("User %s is in cluster %s" % (user, r.userCluster(user)))
    print("Recommendations: %s" % r.mostViewedIntCluster(1))
