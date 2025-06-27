DROP DATABASE IF EXISTS `agri_weather`;
CREATE DATABASE `agri_weather`;
USE `agri_weather`;

CREATE TABLE crop (
  crop_id INT NOT NULL,
  name VARCHAR(50),
  type VARCHAR(50),
  variety VARCHAR(50),
  PRIMARY KEY (crop_id)
);

CREATE TABLE `location` (
  location_id INT NOT NULL,
  county_name VARCHAR(50),
  state VARCHAR(50),
  fips_code INT,
  PRIMARY KEY (location_id)
);

CREATE TABLE season (
  season_id INT NOT NULL,
  name VARCHAR(50),
  start_date DATE,
  end_date DATE,
  PRIMARY KEY (season_id)
);

CREATE TABLE weather_station (
  station_id INT NOT NULL,
  name VARCHAR(50),
  latitude DECIMAL(9,6),
  longitude DECIMAL(9,6),
  location_id INT,
  PRIMARY KEY (station_id),
  FOREIGN KEY (location_id) REFERENCES `location`(location_id)
);

CREATE TABLE weather_event (
  weather_id INT NOT NULL,
  station_id INT NOT NULL,
  date DATE,
  temperature_max INT,
  temperature_min INT,
  precipitation VARCHAR(50),
  wind_speed INT,
  PRIMARY KEY (weather_id),
  FOREIGN KEY (station_id) REFERENCES weather_station(station_id)
);

CREATE TABLE crop_yield (
  yield_id INT NOT NULL,
  crop_id INT,
  season_id INT,
  location_id INT,
  yield_amount FLOAT,
  PRIMARY KEY (yield_id),
  FOREIGN KEY (crop_id) REFERENCES crop(crop_id),
  FOREIGN KEY (season_id) REFERENCES season(season_id),
  FOREIGN KEY (location_id) REFERENCES `location`(location_id)
);

SET SQL_SAFE_UPDATES = 0;

DELETE FROM crop_yield;
DELETE FROM weather_event;
DELETE FROM weather_station;
DELETE FROM location;
DELETE FROM season;
DELETE FROM crop;

SET SQL_SAFE_UPDATES = 1;

INSERT IGNORE INTO crop (crop_id, name, type, variety) VALUES
(101, 'Corn', 'FIELD CROP', 'Dent Corn'),
(102, 'Soybeans', 'FIELD CROP', 'Roundup Ready'),
(103, 'Wheat', 'FIELD CROP', 'Winter Wheat'),
(104, 'Cotton', 'FIELD CROP', 'Upland Cotton');

INSERT IGNORE INTO location (location_id, county_name, state, fips_code) VALUES
(5001, 'Story', 'Iowa', 19169),
(5002, 'Polk', 'Iowa', 19153),
(5003, 'McLean', 'Illinois', 17085),
(6001, 'Adair', 'Missouri', 29001),
(6002, 'Andrew', 'Missouri', 29003),
(6003, 'Atchison', 'Missouri', 29005),
(6004, 'Audrain', 'Missouri', 29007),
(6005, 'Barry', 'Missouri', 29009),
(6006, 'Barton', 'Missouri', 29011),
(6007, 'Bates', 'Missouri', 29013),
(6008, 'Benton', 'Missouri', 29015),
(6009, 'Bollinger', 'Missouri', 29017),
(6010, 'Boone', 'Missouri', 29019),
(6011, 'Buchanan', 'Missouri', 29021),
(6012, 'Butler', 'Missouri', 29023),
(6013, 'Caldwell', 'Missouri', 29025),
(6014, 'Callaway', 'Missouri', 29027),
(6015, 'Cape Girardeau', 'Missouri', 29031),
(6016, 'Carroll', 'Missouri', 29033),
(6017, 'Carter', 'Missouri', 29035),
(6018, 'Cass', 'Missouri', 29037),
(6019, 'Chariton', 'Missouri', 29041),
(6020, 'Christian', 'Missouri', 29043),
(6021, 'Clark', 'Missouri', 29045),
(6022, 'Clay', 'Missouri', 29047),
(6023, 'Clinton', 'Missouri', 29049),
(6024, 'Cole', 'Missouri', 29051),
(6025, 'Cooper', 'Missouri', 29053),
(6026, 'Crawford', 'Missouri', 29055),
(6027, 'Dade', 'Missouri', 29057),
(6028, 'Dallas', 'Missouri', 29059),
(6029, 'Daviess', 'Missouri', 29061),
(6030, 'DeKalb', 'Missouri', 29063),
(6031, 'Dent', 'Missouri', 29065),
(6032, 'Douglas', 'Missouri', 29067),
(6033, 'Dunklin', 'Missouri', 29069),
(6034, 'Franklin', 'Missouri', 29071),
(6035, 'Gasconade', 'Missouri', 29073),
(6036, 'Gentry', 'Missouri', 29075),
(6037, 'Greene', 'Missouri', 29077),
(6038, 'Grundy', 'Missouri', 29079),
(6039, 'Harrison', 'Missouri', 29081),
(6040, 'Henry', 'Missouri', 29083),
(6041, 'Hickory', 'Missouri', 29085),
(6042, 'Holt', 'Missouri', 29087),
(6043, 'Howard', 'Missouri', 29089),
(6044, 'Howell', 'Missouri', 29091),
(6045, 'Iron', 'Missouri', 29093),
(6046, 'Jackson', 'Missouri', 29095),
(6047, 'Jasper', 'Missouri', 29097),
(6048, 'Jefferson', 'Missouri', 29099),
(6049, 'Johnson', 'Missouri', 29101),
(6050, 'Knox', 'Missouri', 29103),
(6051, 'Laclede', 'Missouri', 29105),
(6052, 'Lafayette', 'Missouri', 29107),
(6053, 'Lawrence', 'Missouri', 29109),
(6054, 'Lewis', 'Missouri', 29111),
(6055, 'Lincoln', 'Missouri', 29113),
(6056, 'Linn', 'Missouri', 29115),
(6057, 'Livingston', 'Missouri', 29117),
(6058, 'McDonald', 'Missouri', 29119),
(6059, 'Macon', 'Missouri', 29121),
(6060, 'Madison', 'Missouri', 29123),
(6061, 'Maries', 'Missouri', 29125),
(6062, 'Marion', 'Missouri', 29127),
(6063, 'Mercer', 'Missouri', 29129),
(6064, 'Miller', 'Missouri', 29131),
(6065, 'Mississippi', 'Missouri', 29133),
(6066, 'Moniteau', 'Missouri', 29135),
(6067, 'Monroe', 'Missouri', 29137),
(6068, 'Montgomery', 'Missouri', 29139),
(6069, 'Morgan', 'Missouri', 29141),
(6070, 'New Madrid', 'Missouri', 29143),
(6071, 'Newton', 'Missouri', 29145),
(6072, 'Nodaway', 'Missouri', 29147),
(6073, 'Oregon', 'Missouri', 29149),
(6074, 'Osage', 'Missouri', 29151),
(6075, 'Ozark', 'Missouri', 29153),
(6076, 'Pemiscot', 'Missouri', 29155),
(6077, 'Perry', 'Missouri', 29157),
(6078, 'Pettis', 'Missouri', 29159),
(6079, 'Phelps', 'Missouri', 29161),
(6080, 'Pike', 'Missouri', 29163),
(6081, 'Platte', 'Missouri', 29165),
(6082, 'Polk', 'Missouri', 29167),
(6083, 'Pulaski', 'Missouri', 29169),
(6084, 'Putnam', 'Missouri', 29171),
(6085, 'Ralls', 'Missouri', 29173),
(6086, 'Randolph', 'Missouri', 29175),
(6087, 'Ray', 'Missouri', 29177),
(6088, 'Reynolds', 'Missouri', 29179),
(6089, 'Ripley', 'Missouri', 29181),
(6090, 'Saline', 'Missouri', 29195),
(6091, 'Schuyler', 'Missouri', 29197),
(6092, 'Scotland', 'Missouri', 29199),
(6093, 'Scott', 'Missouri', 29201),
(6094, 'Shannon', 'Missouri', 29203),
(6095, 'Shelby', 'Missouri', 29205),
(6096, 'St. Charles', 'Missouri', 29183),
(6097, 'St. Clair', 'Missouri', 29185),
(6098, 'St. Francois', 'Missouri', 29187),
(6099, 'St. Louis', 'Missouri', 29189),
(6100, 'Ste. Genevieve', 'Missouri', 29193),
(6101, 'Stoddard', 'Missouri', 29207),
(6102, 'Stone', 'Missouri', 29209),
(6103, 'Sullivan', 'Missouri', 29211),
(6104, 'Taney', 'Missouri', 29213),
(6105, 'Texas', 'Missouri', 29215),
(6106, 'Vernon', 'Missouri', 29217),
(6107, 'Warren', 'Missouri', 29219),
(6108, 'Washington', 'Missouri', 29221),
(6109, 'Wayne', 'Missouri', 29223),
(6110, 'Webster', 'Missouri', 29225),
(6111, 'Worth', 'Missouri', 29227),
(6112, 'Wright', 'Missouri', 29229);

INSERT IGNORE INTO season (season_id, name, start_date, end_date) VALUES
(2020, '2020', '2020-01-01', '2020-12-31'),
(2021, '2021', '2021-01-01', '2021-12-31'),
(2022, '2022', '2022-01-01', '2022-12-31');

INSERT IGNORE INTO weather_station (station_id, name, latitude, longitude, location_id) VALUES
  (7041, 'Jackson Weather Station', 39.045, -94.526, 6046),
  (7042, 'Jasper Weather Station', 37.205, -94.353, 6047);

-- Missouri Weather Stations for All Counties
-- Based on representative coordinates for each county and major weather monitoring locations

-- Clear existing weather stations for Missouri (keep your existing 2 if needed)
-- DELETE FROM weather_event WHERE station_id IN (SELECT station_id FROM weather_station WHERE location_id BETWEEN 6001 AND 6112);
-- DELETE FROM weather_station WHERE location_id BETWEEN 6001 AND 6112;

-- Insert weather stations for all Missouri counties
-- Station IDs start from 7100 to avoid conflicts with your existing stations

INSERT IGNORE INTO weather_station (station_id, name, latitude, longitude, location_id) VALUES
(7100, 'Adair County Weather Station', 40.1883, -92.5946, 6001),
(7101, 'Andrew County Weather Station', 39.9542, -94.7972, 6002),
(7102, 'Atchison County Weather Station', 40.2711, -95.0858, 6003),
(7103, 'Audrain County Weather Station', 39.2842, -91.7793, 6004),
(7104, 'Barry County Weather Station', 36.7014, -93.8269, 6005),
(7105, 'Barton County Weather Station', 37.5628, -94.3397, 6006),
(7106, 'Bates County Weather Station', 38.2489, -94.3469, 6007),
(7107, 'Benton County Weather Station', 38.2781, -93.4288, 6008),
(7108, 'Bollinger County Weather Station', 37.3831, -89.8673, 6009),
(7109, 'Boone County Weather Station', 38.9517, -92.3341, 6010),
(7110, 'Buchanan County Weather Station', 39.7392, -94.8469, 6011),
(7111, 'Butler County Weather Station', 36.7403, -90.3384, 6012),
(7112, 'Caldwell County Weather Station', 39.7503, -93.8913, 6013),
(7113, 'Callaway County Weather Station', 38.8214, -91.7001, 6014),
(7114, 'Cape Girardeau County Weather Station', 37.3058, -89.5181, 6015),
(7115, 'Carroll County Weather Station', 39.3667, -93.4955, 6016),
(7116, 'Carter County Weather Station', 36.9342, -91.0176, 6017),
(7117, 'Cass County Weather Station', 38.5656, -94.3441, 6018),
(7118, 'Chariton County Weather Station', 39.5297, -93.0285, 6019),
(7119, 'Christian County Weather Station', 37.0842, -93.2924, 6020),
(7120, 'Clark County Weather Station', 40.1358, -91.7876, 6021),
(7121, 'Clay County Weather Station', 39.2731, -94.4208, 6022),
(7122, 'Clinton County Weather Station', 39.4364, -94.3219, 6023),
(7123, 'Cole County Weather Station', 38.5767, -92.1793, 6024),
(7124, 'Cooper County Weather Station', 38.9742, -92.8285, 6025),
(7125, 'Crawford County Weather Station', 37.9017, -91.4579, 6026),
(7126, 'Dade County Weather Station', 37.4631, -93.8063, 6027),
(7127, 'Dallas County Weather Station', 37.5847, -93.0524, 6028),
(7128, 'Daviess County Weather Station', 39.9189, -93.8180, 6029),
(7129, 'DeKalb County Weather Station', 39.7667, -94.4219, 6030),
(7130, 'Dent County Weather Station', 37.6703, -91.5793, 6031),
(7131, 'Douglas County Weather Station', 36.9753, -92.4796, 6032),
(7132, 'Dunklin County Weather Station', 36.2353, -89.9645, 6033),
(7133, 'Franklin County Weather Station', 38.4375, -90.8568, 6034),
(7134, 'Gasconade County Weather Station', 38.4664, -91.5540, 6035),
(7135, 'Gentry County Weather Station', 40.2200, -94.4119, 6036),
(7136, 'Greene County Weather Station', 37.2081, -93.2923, 6037),
(7137, 'Grundy County Weather Station', 40.1089, -93.6219, 6038),
(7138, 'Harrison County Weather Station', 40.2542, -93.8391, 6039),
(7139, 'Henry County Weather Station', 38.3631, -93.7274, 6040),
(7140, 'Hickory County Weather Station', 37.9797, -93.3716, 6041),
(7141, 'Holt County Weather Station', 40.1239, -95.2397, 6042),
(7142, 'Howard County Weather Station', 39.1217, -92.6724, 6043),
(7143, 'Howell County Weather Station', 36.7975, -91.8551, 6044),
(7144, 'Iron County Weather Station', 37.6614, -90.6290, 6045),
(7145, 'Jackson County Weather Station', 39.0997, -94.5786, 6046),
(7146, 'Jasper County Weather Station', 37.2042, -94.3496, 6047),
(7147, 'Jefferson County Weather Station', 38.2264, -90.4218, 6048),
(7148, 'Johnson County Weather Station', 38.7631, -93.4774, 6049),
(7149, 'Knox County Weather Station', 40.1342, -92.0346, 6050),
(7150, 'Laclede County Weather Station', 37.5342, -92.7607, 6051),
(7151, 'Lafayette County Weather Station', 39.1331, -93.5219, 6052),
(7152, 'Lawrence County Weather Station', 37.0953, -93.8996, 6053),
(7153, 'Lewis County Weather Station', 39.9553, -91.8096, 6054),
(7154, 'Lincoln County Weather Station', 39.1453, -90.9596, 6055),
(7155, 'Linn County Weather Station', 39.8042, -93.0524, 6056),
(7156, 'Livingston County Weather Station', 39.7742, -93.4219, 6057),
(7157, 'McDonald County Weather Station', 36.6831, -94.2774, 6058),
(7158, 'Macon County Weather Station', 39.7442, -92.4746, 6059),
(7159, 'Madison County Weather Station', 37.5553, -90.3929, 6060),
(7160, 'Maries County Weather Station', 38.1253, -91.9018, 6061),
(7161, 'Marion County Weather Station', 39.7553, -91.4957, 6062),
(7162, 'Mercer County Weather Station', 40.3297, -93.5774, 6063),
(7163, 'Miller County Weather Station', 38.3731, -92.4896, 6064),
(7164, 'Mississippi County Weather Station', 36.8542, -89.4129, 6065),
(7165, 'Moniteau County Weather Station', 38.7042, -92.4863, 6066),
(7166, 'Monroe County Weather Station', 39.5453, -91.7240, 6067),
(7167, 'Montgomery County Weather Station', 38.8242, -91.5018, 6068),
(7168, 'Morgan County Weather Station', 38.3464, -92.9607, 6069),
(7169, 'New Madrid County Weather Station', 36.5864, -89.5240, 6070),
(7170, 'Newton County Weather Station', 37.0331, -94.0774, 6071),
(7171, 'Nodaway County Weather Station', 40.3453, -94.8774, 6072),
(7172, 'Oregon County Weather Station', 36.7331, -91.3018, 6073),
(7173, 'Osage County Weather Station', 38.5053, -91.9018, 6074),
(7174, 'Ozark County Weather Station', 36.6331, -92.3274, 6075),
(7175, 'Pemiscot County Weather Station', 36.1531, -89.7274, 6076),
(7176, 'Perry County Weather Station', 37.7242, -89.8729, 6077),
(7177, 'Pettis County Weather Station', 38.7042, -93.2274, 6078),
(7178, 'Phelps County Weather Station', 37.9531, -91.7774, 6079),
(7179, 'Pike County Weather Station', 39.3242, -90.9518, 6080),
(7180, 'Platte County Weather Station', 39.3742, -94.6774, 6081),
(7181, 'Polk County Weather Station', 37.6742, -93.4274, 6082),
(7182, 'Pulaski County Weather Station', 37.8331, -92.2018, 6083),
(7183, 'Putnam County Weather Station', 40.2453, -93.1774, 6084),
(7184, 'Ralls County Weather Station', 39.5942, -91.3518, 6085),
(7185, 'Randolph County Weather Station', 39.4453, -92.4274, 6086),
(7186, 'Ray County Weather Station', 39.3242, -94.0274, 6087),
(7187, 'Reynolds County Weather Station', 37.3331, -90.8774, 6088),
(7188, 'Ripley County Weather Station', 36.6331, -90.8274, 6089),
(7189, 'Saline County Weather Station', 39.1242, -93.2274, 6090),
(7190, 'Schuyler County Weather Station', 40.4453, -92.4774, 6091),
(7191, 'Scotland County Weather Station', 40.4242, -91.8274, 6092),
(7192, 'Scott County Weather Station', 37.0953, -89.4274, 6093),
(7193, 'Shannon County Weather Station', 37.1331, -91.2774, 6094),
(7194, 'Shelby County Weather Station', 39.8042, -92.0274, 6095),
(7195, 'St. Charles County Weather Station', 38.7881, -90.4974, 6096),
(7196, 'St. Clair County Weather Station', 38.0453, -94.2774, 6097),
(7197, 'St. Francois County Weather Station', 37.7742, -90.4774, 6098),
(7198, 'St. Louis County Weather Station', 38.6270, -90.1994, 6099),
(7199, 'Ste. Genevieve County Weather Station', 37.9242, -90.0774, 6100),
(7200, 'Stoddard County Weather Station', 37.0953, -89.8774, 6101),
(7201, 'Stone County Weather Station', 36.7331, -93.6274, 6102),
(7202, 'Sullivan County Weather Station', 40.1953, -93.2774, 6103),
(7203, 'Taney County Weather Station', 36.6331, -92.9774, 6104),
(7204, 'Texas County Weather Station', 37.3331, -91.9774, 6105),
(7205, 'Vernon County Weather Station', 37.8453, -94.3774, 6106),
(7206, 'Warren County Weather Station', 38.7742, -91.1274, 6107),
(7207, 'Washington County Weather Station', 37.8953, -90.6774, 6108),
(7208, 'Wayne County Weather Station', 37.1331, -90.5774, 6109),
(7209, 'Webster County Weather Station', 37.3331, -92.9274, 6110),
(7210, 'Worth County Weather Station', 40.4453, -94.3774, 6111),
(7211, 'Wright County Weather Station', 37.2331, -92.4774, 6112);

-- Add some sample weather events for a few stations to test queries
INSERT IGNORE INTO weather_event (weather_id, station_id, date, temperature_max, temperature_min, precipitation, wind_speed) VALUES
-- St. Louis County samples
(9100, 7198, '2022-07-15', 88, 72, '0.0in', 8),
(9101, 7198, '2022-07-16', 91, 75, '0.2in', 12),
(9102, 7198, '2022-07-17', 93, 78, '0.5in', 15),

-- Greene County (Springfield area) samples  
(9103, 7136, '2022-07-15', 86, 68, '0.1in', 10),
(9104, 7136, '2022-07-16', 89, 71, '0.3in', 14),
(9105, 7136, '2022-07-17', 92, 74, '0.4in', 18),

-- Boone County (Columbia area) samples
(9106, 7109, '2022-07-15', 87, 70, '0.0in', 9),
(9107, 7109, '2022-07-16', 90, 73, '0.1in', 11),
(9108, 7109, '2022-07-17', 92, 76, '0.3in', 13),

-- Cape Girardeau County samples
(9109, 7114, '2022-07-15', 89, 73, '0.2in', 7),
(9110, 7114, '2022-07-16', 92, 76, '0.4in', 9),
(9111, 7114, '2022-07-17', 94, 79, '0.1in', 12);

-- Verify the data with updated query
SELECT 
  l.county_name, 
  l.state, 
  ws.name AS station_name, 
  ws.latitude,
  ws.longitude,
  we.date, 
  we.temperature_max, 
  we.temperature_min, 
  we.precipitation, 
  we.wind_speed
FROM weather_event we
JOIN weather_station ws ON we.station_id = ws.station_id
JOIN location l ON ws.location_id = l.location_id
WHERE l.state = 'Missouri'
ORDER BY l.county_name, we.date;

-- Count weather stations per county
SELECT 
    l.county_name,
    l.state,
    COUNT(ws.station_id) as station_count
FROM location l
LEFT JOIN weather_station ws ON l.location_id = ws.location_id
WHERE l.state = 'Missouri'
GROUP BY l.location_id, l.county_name, l.state
ORDER BY l.county_name;

-- Show all Missouri weather stations
SELECT 
    l.county_name,
    l.state,
    ws.name as station_name,
    ws.latitude,
    ws.longitude
FROM weather_station ws
JOIN location l ON ws.location_id = l.location_id
WHERE l.state = 'Missouri'
ORDER BY l.county_name;

-- Join query to view full weather data
SELECT 
  l.county_name, 
  l.state, 
  ws.name AS station_name, 
  we.date, 
  we.temperature_max, 
  we.temperature_min, 
  we.precipitation, 
  we.wind_speed
FROM weather_event we
JOIN weather_station ws ON we.station_id = ws.station_id
JOIN location l ON ws.location_id = l.location_id
WHERE l.state = 'Missouri';

WITH weather_summary AS (
    SELECT
        l.location_id,
        s.season_id,
        AVG(we.temperature_max) AS avg_tmax,
        AVG(we.temperature_min) AS avg_tmin,
        SUM(CAST(REPLACE(we.precipitation, 'in', '') AS DECIMAL(5,2))) AS total_precip,
        AVG(we.wind_speed) AS avg_wind
    FROM weather_event we
    JOIN weather_station ws ON we.station_id = ws.station_id
    JOIN location l ON ws.location_id = l.location_id
    JOIN season s ON we.date BETWEEN s.start_date AND s.end_date
    GROUP BY l.location_id, s.season_id
)

SELECT
    cy.yield_amount,
    cy.crop_id,
    crop.name AS crop_name,
    l.county_name,
    s.name AS season,
    ws.avg_tmax,
    ws.avg_tmin,
    ws.total_precip,
    ws.avg_wind
FROM crop_yield cy
JOIN crop ON cy.crop_id = crop.crop_id
JOIN location l ON cy.location_id = l.location_id
JOIN season s ON cy.season_id = s.season_id
JOIN weather_summary ws ON ws.location_id = cy.location_id AND ws.season_id = cy.season_id
ORDER BY crop.name, s.name, l.county_name;
