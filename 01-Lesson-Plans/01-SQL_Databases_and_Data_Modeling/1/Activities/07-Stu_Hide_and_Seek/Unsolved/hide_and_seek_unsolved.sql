-- Drop the table if it exists
DROP TABLE IF EXISTS movie_words_comparison;

-- Create table and view column datatypes

-- View the empty table
SELECT * 
FROM movie_words_comparison;

-- Import CSV using pgAdmin menus (no code needed)

-- Collect all rows where "Home Alone (1990)" is in the "reference_title" column

-- Collect all rows where the rater_id is between 10 and 15.

-- Search for the words "artsy" and "heartfelt" in the "soft_attribute" column

-- BONUS
-- Select all rows with a reference title of "Batman (1989)" and a soft attribute of "scary"

-- Collect all rows where the rater is within the 30-40 range and has a reference title of "Home Alone (1990)" and a soft attribute of "artsy"