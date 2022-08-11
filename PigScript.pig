
allText = LOAD '/home/cloudera/Desktop/InputForWC.txt' using TextLoader;
wordsBag = FOREACH allText GENERATE flatten(TOKENIZE((chararray)$0, '\t ')) as word;
wordGroup = GROUP wordsBag BY word;
wordWithCount = FOREACH wordGroup GENERATE group, COUNT(wordsBag.word);
STORE wordWithCount INTO '/home/cloudera/Desktop/output';

users = load '/home/cloudera/Desktop/bdt/l2w3/MovieDataSet/users.txt' using PigStorage('|') as (userId:int,age:int, gender:chararray, occupation:chararray, zipCode:chararray);
maleLawyers = filter users by occupation == 'lawyer' and gender == 'M';
groupAll = group maleLawyers ALL ;
countAllLawyers = foreach groupAll generate COUNT($1);
store countAllLawyers into '/home/cloudera/Desktop/bdt/l2w3/MovieDataSet/soln1/result';


users = load '/home/cloudera/Desktop/bdt/l2w3/MovieDataSet/users.txt' using PigStorage('|') as (userId:int,age:int, gender:chararray, occupation:chararray, zipCode:chararray);
femaleLawyers = order ( filter users by occupation == 'lawyer' and gender == 'F' ) by age desc;
userId = foreach (limit femaleLawyers 1) generate userId;
store userId into '/home/cloudera/Desktop/bdt/l2w3/MovieDataSet/soln2/result';

REGISTER '/usr/lib/pig/piggybank.jar';
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();
movies = load '/home/cloudera/Desktop/bdt/l2w3/MovieDataSet/movies.csv' using CSVExcelStorage(',') as (movieId:int, title:chararray, genre:chararray);
allMoviesWithGenres = foreach movies generate TOKENIZE(genre, '|') as genres, STARTSWITH(title, 'a') as isStartWithLowercaseA, STARTSWITH(title, 'A') as isStartWithUppercaseA;
dump allMoviesWithGenres;
genres = foreach (filter allMoviesWithGenres by isStartWithLowercaseA or isStartWithUppercaseA) generate flatten(genres);
uniqueGroups = order (group genres by $0) by group;
countValue = foreach uniqueGroups generate group, COUNT(genres);
store countValue into '/home/cloudera/Desktop/bdt/l2w3/MovieDataSet/soln3/result';


REGISTER '/usr/lib/pig/piggybank.jar';
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();
movies = load '/home/cloudera/Desktop/bdt/l2w3/MovieDataSet/movies.csv' using CSVExcelStorage(',') as (movieId:int, title:chararray, genre:chararray);
adventureMovies = filter movies by (genre matches '.*Adventure.*');

ratings = load '/home/cloudera/Desktop/bdt/l2w3/MovieDataSet/rating.txt' using PigStorage() as (userId:int , movieId:int, rating:int, timestamp:chararray);
highlyRatedRatings = filter ratings by rating == 5;
highRatings = distinct (foreach highlyRatedRatings generate movieId);

joinRecord = join adventureMovies by movieId, highRatings by movieId;
highRatedAdventureMovies = foreach joinRecord generate adventureMovies::movieId as MovieId, 'Adventure' as Genre, 5 as Rating, adventureMovies::title as Title;

ordered = order  highRatedAdventureMovies by MovieId;
top20 = limit ordered 20;
store top20 into '/home/cloudera/Desktop/bdt/l2w3/MovieDataSet/soln5/result' using org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

