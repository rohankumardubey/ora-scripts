select * 
from
(
select session_id, sql_id, mint start_of_wait, (to_date(to_char(maxt, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD HH24:MI:SS') - to_date(to_char(mint, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD HH24:MI:SS')) * 86400 duration
from
(
  select session_id, sql_id, min(sample_time) mint, max(sample_time) maxt 
  from
  (        
    select session_id, sql_id, sample_time, prev_sample, max(rnum) over (partition by session_id, sql_id order by sample_time asc) grp
    from
    (          
      select session_id, sql_id,
             sample_time,
             lag(sample_time) over ( partition by session_id, sql_id order by sample_time asc) prev_sample,
             case 
               when lag(sample_time) over ( partition by session_id, sql_id order by sample_time asc) is null 
                 or abs(to_date(to_char(lag(sample_time) over ( partition by session_id, sql_id order by sample_time asc), 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD HH24:MI:SS')  - to_date(to_char(sample_time, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD HH24:MI:SS')) > (1/(24*60*60)) then
                 row_number() over ( partition by session_id, sql_id order by sample_time asc)
               else null
             end rnum
      from v$active_session_history
      where event = 'cursor: pin S wait on X'
      and sql_id is not null  
      order by session_id, sample_time, sql_id asc
    )    
  )
  group by session_id, sql_id, grp 
) order by duration desc
) where duration > 5;


-- all events, except a few disk related ones:

select * 
from
(
select session_id, sql_id, event, mint start_of_wait, (to_date(to_char(maxt, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD HH24:MI:SS') - to_date(to_char(mint, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD HH24:MI:SS')) * 86400 duration
from
(
  select session_id, sql_id, event, min(sample_time) mint, max(sample_time) maxt 
  from
  (        
    select session_id, sql_id, event, sample_time, prev_sample, max(rnum) over (partition by session_id, sql_id, event order by sample_time asc) grp
    from
    (          
      select session_id, sql_id, event,
             sample_time,
             lag(sample_time) over ( partition by session_id, sql_id, event order by sample_time asc) prev_sample,
             case 
               when lag(sample_time) over ( partition by session_id, sql_id, event order by sample_time asc) is null 
                 or abs(to_date(to_char(lag(sample_time) over ( partition by session_id, sql_id, event order by sample_time asc), 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD HH24:MI:SS')  - to_date(to_char(sample_time, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD HH24:MI:SS')) > (1/(24*60*60)) then
                 row_number() over ( partition by session_id, sql_id, event order by sample_time asc)
               else null
             end rnum
      from v$active_session_history
      where event not in ('db file sequential read', 'db file scattered read', 'direct path read temp', 'direct path read') 
      and sql_id is not null  
      order by session_id, sample_time, sql_id asc
    )    
  )
  group by session_id, sql_id, event, grp 
) order by duration desc
) where duration > 20
order by start_of_wait desc;