"""
Architecture:

movies.csv -> Publisher

movies.subscription -> subscribed to -> Publisher

movies.subscription -> send movie data ->
    Processor (Apache Beam) -> filter out by genre (i.e. comedy)
        -> comedy_movies

comedy.movies.subscription -> Subscriber -> Console


"""
