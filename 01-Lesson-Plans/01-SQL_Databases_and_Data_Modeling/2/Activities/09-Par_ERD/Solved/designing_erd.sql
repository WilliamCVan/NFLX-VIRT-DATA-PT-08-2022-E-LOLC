CREATE TABLE gym (
    gym_id INTEGER   NOT NULL,
    gym_name VARCHAR   NOT NULL,
    address VARCHAR   NOT NULL,
    city VARCHAR   NOT NULL,
    zipcode VARCHAR   NOT NULL,
    PRIMARY KEY (gym_id)
);

CREATE TABLE trainers (
    trainer_id INTEGER   NOT NULL,
    gym_id INTEGER   NOT NULL,
    first_name VARCHAR   NOT NULL,
    last_name VARCHAR   NOT NULL,
    PRIMARY KEY (trainer_id),
    FOREIGN KEY(gym_id) REFERENCES gym (gym_id)
);

CREATE TABLE members (
    member_id INTEGER   NOT NULL,
    gym_id INTEGER   NOT NULL,
    trainer_id INTEGER   NOT NULL,
    first_name VARCHAR   NOT NULL,
    last_name VARCHAR   NOT NULL,
    address VARCHAR   NOT NULL,
    city VARCHAR   NOT NULL,
    PRIMARY KEY (member_id),
    FOREIGN KEY(gym_id) REFERENCES gym (gym_id),
    FOREIGN KEY(trainer_id) REFERENCES trainers (trainer_id)
);

CREATE TABLE payments (
    payment_id INTEGER   NOT NULL,
    member_id INTEGER   NOT NULL,
    creditcard_info INTEGER   NOT NULL,
    billing_zip INTEGER   NOT NULL,
    PRIMARY KEY (payment_id),
    FOREIGN KEY(member_id) REFERENCES Members (member_id)
);
