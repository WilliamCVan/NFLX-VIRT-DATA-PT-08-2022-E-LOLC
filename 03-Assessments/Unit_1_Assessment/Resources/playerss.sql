Create table players (
	player_id INT NOT NULL,
	first_name VARCHAR(100) NOT NULL,
	late_name VARCHAR(100) NOT NULL,
	hand VARCHAR(25) NOT NULL,
	country_code VARCHAR(25) NOT NULL
);

select * from players

select hand, count(*) FROM players 
GROUP BY hand


SELECT count(*) FROM players
JOIN matches ON matches.winner_id = players.player_id 
GROUP BY hand.players,
FROM players



