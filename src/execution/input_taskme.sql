create table tb(s int, a int, b float, c char(16));
insert into tb values (0, 1, 1.2, 'abc');
insert into tb values (2, 2, 2.0, 'def');
insert into tb values (5, 3, 2., 'xyz');
create index tb (a);
desc tb;
select * from tb where a>=1;
#