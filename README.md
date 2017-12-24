# sqlCsvAsap
Perform SQL queries on CSV data. Project for Fall 2017 CS411 Database Systems class at University of Illinois.

To make queries go faster, index available tables on the attributes you want to query. To do so, type 'index' from the start menu. One version of indexing is available currently: keyword indexing.

Sample index command:

    create index TABLENAME keyword ATTRIBUTENAME
    create index business keyword city

Queries take arguments for SELECT, FROM, and WHERE. Joins are handled in the WHERE clause, for example, by setting WHERE table1.attr = table2.attr. SELECT clauses can include DISTINCT to remove duplicates from final results. Table names can be aliased in FROM, and alias names used elsewhere in the query.

Here are some test queries to try:

    SELECT * FROM BostonMarathon2017
    SELECT Name, Year, Award, Winner FROM oscars WHERE (Year = 2013) AND (Winner = 1)
    SELECT DISTINCT Year, Film, movie_title, title_year, director_name, Award FROM oscars, movies WHERE Name = director_name AND Year > title_year + 5 AND Winner = 1
    SELECT B.name, B.postal_code, R.review_id, R.stars, R.useful FROM business B, review-1m R WHERE B.city = Champaign AND B.state = IL AND B.business_id = R.business_id


If you would like to see more information about what the program is doing, in utils.py, set the TESTMODE variable to True.
