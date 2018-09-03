CREATE TABLE car (
    id INT AUTO_INCREMENT PRIMARY KEY,
    vin CHAR(17),
    stock_number VARCHAR(20),
    type CHAR(1),
    model_year SMALLINT UNSIGNED,
    make VARCHAR(25),
    model VARCHAR(25),
    trim VARCHAR(60),
    body_type VARCHAR(20),
    drive CHAR(3),
    displacement SMALLINT UNSIGNED,
    mileage INT UNSIGNED,
    ext_color VARCHAR(50),
    int_color VARCHAR(150),
    transmission CHAR(1),
    fuel CHAR(1),
    price INT UNSIGNED,
    equipment VARCHAR(5000),
    carproof VARCHAR(150),
    thumbnail VARCHAR(300),
    dealer VARCHAR(50));
	
CREATE INDEX mileage_idx
ON car (mileage);

CREATE INDEX price_idx
ON car (price);
