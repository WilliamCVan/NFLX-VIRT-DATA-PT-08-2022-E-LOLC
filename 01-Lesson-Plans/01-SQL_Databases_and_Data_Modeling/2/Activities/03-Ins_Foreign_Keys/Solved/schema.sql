CREATE TABLE owners (
  id SERIAL PRIMARY KEY,
  name VARCHAR(30) NOT NULL
);


INSERT INTO owners (name)
VALUES
  ('Bob'),
  ('Micaela'),
  ('Aquaman');

SELECT * FROM owners;


CREATE TABLE animals (
  id SERIAL PRIMARY KEY,
  species VARCHAR(30),
  name VARCHAR(30),
  owner_id VARCHAR(30) NOT NULL,
  FOREIGN KEY(owner_id) REFERENCES owners(id)
);

INSERT INTO animals (species, name, owner_id)
VALUES
  ('Dog', 'Spot', 1),
  ('Fish', 'Goldy', 1),
  ('Cat', 'Loki', 2),
  ('Dolphin', 'Jeffrey', 3);

SELECT * FROM animals;

-- Insert error
INSERT INTO animals (species, name, owner_id)
VALUES ('Otter', 'River', 4);

-- Correct insert
INSERT INTO owners (name)
VALUES
  ('Dave');

INSERT INTO animals (species, name, owner_id)
VALUES
  ('Otter', 'River', 4);

SELECT * FROM animals;
