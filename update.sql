use mapa;
delete from casos where id_evento_caso != 0;
LOAD DATA LOCAL INFILE '/srv/http/Covid19Casos.csv' INTO TABLE casos columns TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n'  IGNORE 1 LINES;
delete from casos where length(edad) > 3;
