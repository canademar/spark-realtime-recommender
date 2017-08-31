#!/usr/bin/redis
import operator
from collections import Counter
from redis import Redis


class RedisUtils:


    def __init__(self, host, port):
        self.redis = Redis(host, port)

    def user_cluster(self, user):       
        return self.redis.hget("clusterAssignment", user).decode("utf8")

    def cluster_most_viewed(self, cluster):
        print("Cluster most viewed for %s" % cluster)
        clusterKeys = self.redis.keys("cluster_%s*" % cluster)
        clusterKeys = list(map(lambda x: x.decode("utf8").split("_")[3], clusterKeys))
        counted = Counter(clusterKeys)
        print("counted keys:%s " % counted)
        sortedItems = sorted(counted, key=counted.get, reverse=True)
        print("sorted: %s" % sortedItems)
        return sortedItems[0:10]
        

if __name__=="__main__":
    print("Hi")
    r = RedisUtils("localhost", 6379)
    user = "1"
    cluster = r.user_cluster(user)
    print("User %s is in cluster %s" % (user, cluster))
    print("Recommendations: %s" % r.cluster_most_viewed(cluster))
