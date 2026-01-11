INSERT INTO users (email, country_code)
VALUES
('alice@test.com','SE'),
('bob@test.com','DE'),
('carla@test.com','FR');

INSERT INTO orders (user_id,status,total_amount,currency,ordered_at)
VALUES
(1,'CONFIRMED',120.50,'EUR',now()-interval '3 days'),
(2,'CANCELLED',80.00,'EUR',now()-interval '2 days'),
(1,'CONFIRMED',300.00,'EUR',now()-interval '1 day');
