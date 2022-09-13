Create table matches (
	loser_age FLOAT NOT NULL,
	loser_id INT NOT NULL,
	loser_name VARCHAR(100) NOT NULL,
	loser_rank INT NOT NULL,
	winner_age FLOAT NOT NULL, 
	winner_id INT NOT NULL,
	winner_name VARCHAR(100) NOT NULL,
	winner_rank INT NOT NULL
);

select * from matches



select winner_name, count(*) as total FROM matches 
GROUP BY winner_name
ORDER BY total DESC


select loser_name, count(*) as total FROM matches 
GROUP BY loser_name
ORDER BY total DESC

select loser_name, count(*) as total
FROM matches 
GROUP BY loser_name
ORDER BY loser_age DESC



