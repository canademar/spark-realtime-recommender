from flask import Blueprint
main = Blueprint('main', __name__)
 
import json
from engine import RecommendationEngine
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
from flask import Flask, request
 
@main.route("/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
def top_ratings(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.get_top_ratings(user_id,count)
    return json.dumps(top_ratings)
 
@main.route("/<int:user_id>/ratings/<int:movie_id>", methods=["GET"])
def movie_ratings(user_id, movie_id):
    logger.debug("User %s rating requested for movie %s", user_id, movie_id)
    ratings = recommendation_engine.get_ratings_for_movie_ids(user_id, [movie_id])
    return json.dumps(ratings)
 
@main.route("/<int:user_id>/cluster/cluster", methods=["GET"])
def user_cluster(user_id):
    logger.debug("User cluster", user_id)
    user_cluster= recommendation_engine.user_cluster(user_id)
    return json.dumps({"user_cluster": user_cluster})

@main.route("/<int:user_id>/cluster/mostViewed", methods=["GET"])
def recommendation_by_cluster(user_id):
    logger.debug("User recommendation by cluster", user_id)
    user_cluster= recommendation_engine.user_cluster(user_id)
    print("\n\n\n\n user_cluster:%s" % user_cluster)
    most_viewed = recommendation_engine.get_most_viewed_by_cluster(user_id)
    print("\n\n\n\n most_viewed:%s" % most_viewed)
    return json.dumps({"user_cluster":user_cluster, "recommended":most_viewed})
 
@main.route("/<int:user_id>/ratings", methods = ["POST"])
def add_ratings(user_id):
    # get the ratings from the Flask POST request object
    print(request.form.keys())
    print(type(request.form.keys()))
    ratings_list = []
    for key in request.form.keys():
        print(key)
        pairs = key.strip().split("\n")
        ratings_list = map(lambda x: x.split(","), pairs)
    print(ratings_list)
    # create a list with the format required by the negine (user_id, movie_id, rating)
    ratings = map(lambda x: (user_id, int(x[0]), float(x[1])), ratings_list)
    # add them to the model using then engine API
    recommendation_engine.add_ratings(ratings)
    print(ratings)
    print(list(ratings))
    return ', '.join("%s:%r" % (key,val) for (key,val) in list(ratings))
    #return json.dumps(ratings)
 
 
def create_app(spark_context, dataset_path):
    global recommendation_engine 

    recommendation_engine = RecommendationEngine(spark_context, dataset_path)    
    
    app = Flask(__name__)
    app.register_blueprint(main)
    return app 
