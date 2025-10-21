-- Load CSV from HDFS
students = LOAD '/user/hadoop/input/students.csv'
           USING PigStorage(',')
           AS (id:int, name:chararray, marks:int);

-- Sort students by marks descending
sorted_students = ORDER students BY marks DESC;

-- Store output back to HDFS
STORE sorted_students INTO '/user/hadoop/output/sorted_students'
       USING PigStorage(',');
