# sqlCsvAsap
Perform SQL queries on CSV data. Project for Fall 2017 CS411 Database Systems class at University of Illinois.

START

Put any CSV files that you want to query in a folder called tables in the same directory as the code files. (For class evaluation, also take the /index folder, including subfolders and files, and put the index folder in the same directory as the code files.)

Start the program by running sqlCsvAsap.py. 

QUERYING

To start a query, type 'query' from the start menu.

Queries take arguments for SELECT, FROM, and WHERE. Joins are handled in the WHERE clause, for example, by setting WHERE table1.attr = table2.attr. (Note: the program currenty does not handle joins on the same table.) SELECT clauses can include DISTINCT to remove duplicates from final results. Table names can be aliased in FROM, and alias names used elsewhere in the query.

Here are some test queries to try:

    SELECT * FROM BostonMarathon2017
    SELECT Name, Year, Award, Winner FROM oscars WHERE (Year = 2013) AND (Winner = 1)
    SELECT DISTINCT Year, Film, movie_title, title_year, director_name, Award FROM oscars, movies WHERE Name = director_name AND Year > title_year + 5 AND Winner = 1
    SELECT B.name, B.postal_code, R.review_id, R.stars, R.useful FROM business B, review-1m R WHERE B.city = Champaign AND B.state = IL AND B.business_id = R.business_id
    
INDEXING

To make queries go faster, index available tables on the attributes you want to query.

To create an index, type 'index' from the start menu. One version of indexing is available currently: keyword indexing.

Sample index command:

    create index TABLENAME keyword ATTRIBUTENAME
    create index business keyword city

FOR MORE INFORMATION

If you would like to see more information about what the program is doing, in utils.py, set the TESTMODE variable to True.
