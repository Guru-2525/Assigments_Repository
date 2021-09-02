-- In this project I am using mysql workbench .
-- In mysql ,creating a schema is similar to creating a new database.
 
/* Creating a database called mulesoft _db */


create schema mulesoft_db ;


/* making sure that we are connected to the database that we have created */

use mulesoft_db ;


/* creating a new table in that database */
       -- with fields id,movie_name,actor,actress,year of release,director.alter
  
create table movies (movie_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                     movie_name VARCHAR(100) NOT NULL,
                     lead_actor VARCHAR(50) NOT NULL,
                     lead_actress VARCHAR(50) NOT NULL,
                     release_year INT,
                     director VARCHAR(50) NOT NULL);
                     
                     
/*Inserting the data of movies into the table that we have created earlier  */

INSERT INTO  movies(
                      movie_name,lead_actor,lead_actress,release_year,director
					)
			VALUES ('Mayabazaar','NTR','Savitri',1957,'Kadiri Venkata Reddy'),
                   ('Jersy','Nani','Shraddha Srinath',2019,'Gowtam Tinnanuri'),
                   ('Mahanati','Dulquer Salmaan','Keerthy Suresh',2018,'Nag Ashwin'),
                   ('Bahubali','Prabhas','Anushka Shetty',2015,'Raj Mouli'),
                   ('Bahubali-2','Prabhas','Anushka Shetty',2017,'Raj Mouli'),
                   ('KGF','Yash','Srinidhi Shetty',2018,'Prashanth Neel'),
                   ('Arjun Reddy','Vijay Devarakonda','Shalini Pandey',2017,'Sandeep Reddy'),
                   ('Magadheera','Ram Charan','Kajal',2009,'Raj Mouli'),
                   ('Dangal','Amir Khan','Sakshi Tanwar',2016,'Nitesh Tiwari'),
                   ('Saaho','Prabhas','Shraddha Kapoor',2019,'Sujeeth');
                   
/* Getting the whole data from the table movies by using a simple select statement */

SELECT * 
FROM movies ;

/* Querying the data from the table by applying some filters */
  -- 1
SELECT *
FROM movies 
WHERE lead_actor LIKE '%prabhas%' ;

-- 2
SELECT * 
FROM movies 
WHERE release_year >2000;

-- 3
SELECT * 
FROM movies 
ORDER BY movie_name ;

-- 4 
SElECT distinct(lead_actor)
FROM movies;





                     
                    