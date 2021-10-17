use local;

create database movies;
create database genome;

create table movies.ratings (
                                userId string,
                                movieId string,
                                rating string,
                                timestamp string
);

create table movies.tags (
                             userId string,
                             movieId string,
                             tag string,
                             timestamp string
);

create table movies.movies (
                               movieId string,
                               title string,
                               genres string
);

create table movies.links (
                              movieId string,
                              imdbId string,
                              tmdbId string
);

create table genome.scores (
                               movieId string,
                               tagId string,
                               relevance string
);

create table genome.tags (
                             tagId string,
                             tag string
);