--
-- Enable / Disable tracing in another session
--

-- Get the sid and serial# and plug them into this script

begin
  sys.dbms_system.set_bool_param_in_session(sid => 123,
					    serial# => 40044,
					    parnam => 'timed_statistics',
					    bval => true);

	sys.dbms_system.set_int_param_in_session(sid => 123,
					         serial# => 40044,
					         parnam => 'max_dump_file_size',
					         intval => 2147483647);
  --                     sid  serial                    										 
  sys.dbms_system.set_ev(123, 40044, 10046, 8, '');
end;
/


-- stop tracing

begin
  --                     sid  serial
  sys.dbms_system.set_ev(123, 40044, 10046, 0, '');
end;
/


--
-- Enable / Disable tracing in your own session
--

ALTER SESSION SET timed_statistics=TRUE;
ALTER SESSION SET max_dump_file_size=UNLIMITED;
ALTER SESSION SET tracefile_identifier='sodonnel_wa_pd_';
ALTER SESSION SET EVENTS '10046 trace name context forever, level 8';

  execute immediate 'ALTER SESSION SET timed_statistics=TRUE';
  execute immediate 'ALTER SESSION SET max_dump_file_size=UNLIMITED';
  execute immediate 'ALTER SESSION SET tracefile_identifier=''sodonnel_''';
  execute immediate 'ALTER SESSION SET EVENTS ''10046 trace name context forever, level 8''';

-- code to trace here.

ALTER SESSION SET EVENTS '10046 trace name context off';