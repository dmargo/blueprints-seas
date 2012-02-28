CREATE USER 'dmargo'@'localhost' IDENTIFIED BY 'kitsune';
CREATE DATABASE graphdb;
GRANT ALL ON graphdb.* TO 'dmargo'@'localhost' WITH GRANT OPTION;

