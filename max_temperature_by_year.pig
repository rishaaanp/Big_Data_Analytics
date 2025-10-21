-- Load the CSV file using comma as delimiter
data = LOAD '/user/hadoop/input/temperature_data.csv' 
    USING PigStorage(',') 
    AS (date:chararray, temperature:int);

-- Remove the header row
filtered = FILTER data BY date != 'Date';

-- Extract year (first 4 characters of date)
with_year = FOREACH filtered GENERATE SUBSTRING(date, 0, 4) AS year, temperature;

-- Group records by year
grouped = GROUP with_year BY year;

-- Find maximum temperature for each year
max_temp = FOREACH grouped GENERATE group AS year, MAX(with_year.temperature) AS max_temperature;

-- Store the result into output directory
STORE max_temp INTO '/user/hadoop/output/max_temperature_by_year' USING PigStorage(',');
