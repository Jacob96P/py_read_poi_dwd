DROP TABLE IF EXISTS dwd_poi;
CREATE TABLE IF NOT EXISTS dwd_poi
(
	id SERIAL PRIMARY KEY,
	stationsid INT,
	stationsname VARCHAR,
	zeitpunkt TIMESTAMP,
	Wolkenbedeckung INT,
	Niederschlag_letzte_Stunde FLOAT,
	Temperatur FLOAT,
	Windboen_letzte_Stunde INT,
	Windgeschwindigkeit INT,
	Windrichtung INT,
	Druck_auf_Meereshoehe FLOAT,
	shape GEOMETRY,
    modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

