We have two queries we want to try and 'fake' into take full table scans.

The first is:

SELECT          /*+ */
       DISTINCT "A1"."M_ROW$$"
           FROM "GENEVA_ADMIN"."MLOG$_BILLSUMMARY" "A1"
          WHERE "A1"."M_ROW$$" <> ALL (SELECT "A2".ROWID
                                         FROM "GENEVA_ADMIN"."BILLSUMMARY" "A2"
                                        WHERE "A2".ROWID = "A1"."M_ROW$$")
            AND "A1"."SNAPTIME$$" > :1
            AND "A1"."DMLTYPE$$" <> 'I'
            
The second is currently unknown - we can deal with it after.

The current plan is:

---------------------------------------------------------------
| Id  | Operation                     | Name                  |
---------------------------------------------------------------
|   0 | SELECT STATEMENT              |                       |
|   1 |  HASH UNIQUE                  |                       |
|   2 |   FILTER                      |                       |
|   3 |    TABLE ACCESS BY INDEX ROWID| MLOG$_BILLSUMMARY     |
|   4 |     INDEX RANGE SCAN          | MLOG$_BILLSUMMARY_AK1 |
|   5 |    TABLE ACCESS BY USER ROWID | BILLSUMMARY           |


What we want, is to see a full table scan on both the MLOG$ table and BILLSummary, something like:


select count(*)
from
(
SELECT          /*+ full(a1) */
       DISTINCT "A1"."M_ROW$$"
           FROM "GENEVA_ADMIN"."MLOG$_BILLSUMMARY" "A1"
          WHERE "A1"."M_ROW$$" <> ALL (SELECT /*+ full(a2) */ "A2".ROWID
                                         FROM "GENEVA_ADMIN"."BILLSUMMARY" "A2"
                                        WHERE "A2".ROWID = "A1"."M_ROW$$")
            AND "A1"."SNAPTIME$$" > :1
            AND "A1"."DMLTYPE$$" <> 'I'
);            
            
-------------------------------------------------
| Id  | Operation           | Name              |
-------------------------------------------------
|   0 | SELECT STATEMENT    |                   |
|   1 |  HASH UNIQUE        |                   |
|   2 |   FILTER            |                   |
|   3 |    TABLE ACCESS FULL| MLOG$_BILLSUMMARY |
|   4 |    TABLE ACCESS FULL| BILLSUMMARY       |
-------------------------------------------------


Note the query above has had hints added to force that plan.

To get the plan we want, we have to use PRIVATE outlines, and then promote them to PUBLIC outlines.


1.

Create the private outline (require create any outline priv)

create or replace private outline MVBILLSUM_R1_Y on
SELECT          /*+ */
       DISTINCT "A1"."M_ROW$$"
           FROM "GENEVA_ADMIN"."MLOG$_BILLSUMMARY" "A1"
          WHERE "A1"."M_ROW$$" <> ALL (SELECT "A2".ROWID
                                         FROM "GENEVA_ADMIN"."BILLSUMMARY" "A2"
                                        WHERE "A2".ROWID = "A1"."M_ROW$$")
            AND "A1"."SNAPTIME$$" > :1
            AND "A1"."DMLTYPE$$" <> 'I';
            
create or replace private outline MVBILLSUM_R1_N on            
SELECT          /*+ full(a1) */
       DISTINCT "A1"."M_ROW$$"
           FROM "GENEVA_ADMIN"."MLOG$_BILLSUMMARY" "A1"
          WHERE "A1"."M_ROW$$" <> ALL (SELECT /*+ index(a2) */ "A2".ROWID
                                         FROM "GENEVA_ADMIN"."BILLSUMMARY" "A2"
                                        WHERE "A2".ROWID = "A1"."M_ROW$$")
            AND "A1"."SNAPTIME$$" > :1
            AND "A1"."DMLTYPE$$" <> 'I';
            
now run:

select hint#, hint_text
from ol$hints
where ol_name = 'MVBILLSUM_R1_Y';

Note that hint# 1 and 2 show the index scan and rowid lookup.

Now run:

select hint#, hint_text
from ol$hints
where ol_name = 'MVBILLSUM_R1_N';

Note how hint# 1 and hint#2 are fulls scans - we need to copy these into the place of those in first outline.

update ol$hints
set hint_text = (select hint_text from ol$hints where hint# = 1 and ol_name = 'MVBILLSUM_R1_N')
where hint# = 1
and ol_name = 'MVBILLSUM_R1_Y'; 

update ol$hints
set hint_text = (select hint_text from ol$hints where hint# = 2 and ol_name = 'MVBILLSUM_R1_N')
where hint# = 2
and ol_name = 'MVBILLSUM_R1_Y'; 

commit;

Ensure the new outline is flushed to disk:

exec dbms_outln_edit.refresh_private_outline('MVBILLSUM_R1_Y'); 

Now we need to test the outline, so we need to tell the session to use private outlines:

alter session set use_private_outlines = true;

Explain the original query - It should now take the plan we want.

Now set use_private_outlines = false and explain the query again - it should fall back to the old plan.

alter session set use_private_outlines = false;

Explain the query again.

Now, promote the outline to a public outline:

create public outline MVBILLSUM_R1 from private MVBILLSUM_R1_Y;


Explain the query - it should take the outline plan.

Done.
===========

            
            
            

            
