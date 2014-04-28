create table debugtab (log varchar2(4000), dtm date);

create or replace procedure debugit(value varchar2)
as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
  insert into debugtab (log, dtm) values (value, sysdate);
  commit;
end;
/

grant execute on debugit to public;