import requests
import operator
import json

CUBES_SERVICES_IP = "localhost"
CUBES_SERVICES_PORT = 4321

def context_sort(ratings, context):
    weighted = []
    for rating in ratings:
        weight = rating_weight_in_context(rating, context)
        weighted.append((rating, weight))
    weighted.sort(key=operator.itemgetter(1))
    print("context_sorted %s" % weighted)
    return [s[0] for s in weighted]


def rating_weight_in_context(rating, context):
    item_id = rating["id"]
    response = requests.get("http://%s:%s/item/%s" % (CUBES_SERVICES_IP, CUBES_SERVICES_PORT, item_id))
    parsed = json.loads(response.text)
    for item in parsed["aggregation"]:
        if item["time"]==context:
            return item["record_count"]
    return 0
    


if __name__=='__main__':
    rating = {"id":10}
    weight = rating_weight_in_context(rating, 5)
    print("Weight: %s" % weight)
