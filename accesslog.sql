CREATE TABLE accesslog
(
    ID INT(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,
    Date DATETIME NOT NULL,
    IP VARCHAR(15) NOT NULL,
    Request VARCHAR(50) NOT NULL,
    Status INT(11) NOT NULL,
    UserAgent VARCHAR(1024) NOT NULL
);

select * from accesslog where Date between '2017-01-01 13:00:00' and '2017-01-01 14:00:00' group by IP having count(*) > 100;
select * from accesslog where Date between '2017-01-01 13:00:00' and '2017-01-02 13:00:00' group by IP having count(*) > 250;
select * from accesslog where IP='192.168.228.188';