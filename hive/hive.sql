# You sql follows

# create table esttudiante
create table estudiantes(region string, distrito string, school_id string, school_name string, nivel string, sexo string, student_id string) 
row format delimited fields terminated by ',';

# create table escuelas
create table escuelas(region string, distrito string, ciudad string, school_id string, name string, nivel string, collegeboard_id string) row format delimited fields terminated by ',';

# import data to table estudiantes
load data local inpath '/home/ubuntu/eclipse-workspace/exam1-sp17-bigdata-desc/hive/studentsPR.csv' overwrite into table estudiantes;

# import data to table escuelas
load data local inpath '/home/ubuntu/eclipse-workspace/exam1-sp17-bigdata-desc/hive/escuelasPR.csv' overwrite into table escuelas;

# query 1
# find total number of students per region and city
create table studentsPerRegionAndCity as
select S.region, S.ciudad, count(*) 
from estudiantes as E, escuelas as S 
where E.school_id=S.school_id
group by S.region, S.ciudad;

# query 2
# find total number of schools per city and level
create table schoolsPerCityAndLevel as
select city, level, count(*)
from estudiantes
group by city, level;

#quey 3
# find female students from Ponce and a Superior
create table femaleStudentsPonceSuperior as
select count(*)
from estudiantes as E, escuelas as S
where E.shool_id=S.school_id
and S.ciudad='Ponce'
and S.nivel='Superior';

# query 4
# frind male students per region, district and city
create table maleStudentsPerRegionDistrictCity as
select E.region, E.distrito, S.ciudad, count(*)
from estudiantes as E, escuelas as S
where E.sexo='M'
group by E.region, E.distrito, S.ciudad;
