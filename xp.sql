set lines 250
set pages 250

select * from table(dbms_xplan.display());