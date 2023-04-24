"""

Data downloaded from --> https://grouplens.org/datasets/movielens/
Dataset -> https://files.grouplens.org/datasets/movielens/ml-latest.zip

Architecture
movies.csv -> Publisher

movies.subscription -> subscribed to -> Publisher

movies.subscription -> send movie data ->
    Processor (Apache Beam) -> filter out by genre (i.e. comedy)
        -> comedy_movies

comedy.movies.subscription -> Subscriber -> Console


"""
