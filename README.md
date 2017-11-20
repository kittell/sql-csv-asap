# sqlCsvAsap
Perform SQL queries on CSV data. Project for Fall 2017 CS411 Database Systems class at University of Illinois.

Here are some test queries to try:

    SELECT Name, Year, Winner FROM oscars WHERE (Year = 2013) AND (Winner = 1)
    SELECT Name FROM BostonMarathon2017 WHERE (Country = KEN)
    SELECT Name FROM BostonMarathon2017 WHERE (Country = KEN) AND (Gender = F)
    SELECT Year, Film, Name FROM oscars WHERE (Winner = 1) AND (Award = Film Editing)
    SELECT * FROM BostonMarathon2017 WHERE (Name LIKE %Mc%) AND (State <> IL)
