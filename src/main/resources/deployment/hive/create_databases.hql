-- Create production databases for landing, base and analytical layers.

-- Create database for landing layer
CREATE DATABASE IF NOT EXISTS tweeter_landing
LOCATION '/interns/prod/landing/tweeter_landing';

-- Create database for base layer
CREATE DATABASE IF NOT EXISTS tweeter_base
LOCATION '/interns/prod/base/tweeter_base';

-- Create database for analytical layer
CREATE DATABASE IF NOT EXISTS tweeter_analytical
LOCATION '/interns/prod/analytical/tweeter_analytical';