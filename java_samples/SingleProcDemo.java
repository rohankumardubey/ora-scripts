/*

create table payments (payment_id     integer not null,
                       payment_amount number,
                       payment_date   date,
                       card_number    varchar2(20),
                       expire_month   varchar2(2),
                       expire_year    varchar2(2),
                       name_on_card   varchar2(50)
                       );

create unique index payments_idx1 on payments(payment_id);

alter table payments add constraint payments_pk
  primary key (payment_id) using index;

create sequence payments_seq
start with 1
increment by 1
cache 10000;


create or replace type integer_t is table of integer
/

create or replace type varchar2_t is table of varchar2(255)
/

create or replace type number_t is table of number
/

Create or replace procedure insert_payments_a(i_payment_amount   number_t,
                                              i_card_number      varchar2_t,
                                              i_expire_month     varchar2_t,
                                              i_expire_year      varchar2_t,
                                              i_name_on_card     varchar2_t,
                                              o_errors       out integer_t)
is
 dml_errors EXCEPTION;
 PRAGMA EXCEPTION_INIT(dml_errors, -24381);
begin
  -- need to initialize the collection or it will fail if it is used
  o_errors := INTEGER_T();
  forall i in 1 .. i_payment_amount.last save exceptions
  insert into payments        (payment_id,
                               payment_amount,
                               payment_date,
                               card_number,
                               expire_month,
                               expire_year,
                               name_on_card
                              )
                      values (
                               payments_seq.nextval,
                               i_payment_amount(i),
                               sysdate,
                               i_card_number(i),
                               i_expire_month(i),
                               i_expire_year(i),
                               i_name_on_card(i)
                              );
exception
  when dml_errors then
    for i in 1 .. SQL%bulk_exceptions.count loop
      --if SQL%BULK_EXCEPTIONS(i).ERROR_CODE != 00001 then
      --  debug.f( '  ... An unexpected exception occurred (%s)', SQLERRM );
      --  raise;
      --end if;
      o_errors.extend(1);
      o_errors(o_errors.count) := SQL%bulk_exceptions(i).ERROR_INDEX;
    end loop;
end insert_payments_a;
/

*/

import oracle.jdbc.*;
import oracle.sql.ArrayDescriptor;
import oracle.sql.ARRAY;
import oracle.jdbc.OracleTypes;
import java.sql.*;

public class SingleProcDemo {
    
    public static void main(String[] args)
	throws ClassNotFoundException, SQLException, InterruptedException
    {
        DriverManager.registerDriver (new oracle.jdbc.OracleDriver());
	
        String url = "jdbc:oracle:thin:@//localhost:1521/local11gr2.world";
	
        Connection conn =
	    DriverManager.getConnection(url,"sodonnel","sodonnel");

        conn.setAutoCommit(false);
	
        // Create descriptors for each Oracle collection type required
        ArrayDescriptor oracleVarchar2Collection =
            ArrayDescriptor.createDescriptor("VARCHAR2_T",conn);

	ArrayDescriptor oracleIntegerCollection =
            ArrayDescriptor.createDescriptor("INTEGER_T",conn);

	ArrayDescriptor oracleNumberCollection =
            ArrayDescriptor.createDescriptor("NUMBER_T",conn);

	//        CallableStatement cstmt = 
        //    conn.prepareCall("{ call insert_payments_a(?, ?, ?, ?, ?, ? ) }"); 

	long start_of_single_row = System.currentTimeMillis();

        PreparedStatement stmt = 
            conn.prepareStatement("insert into payments (payment_id, payment_amount, payment_date, card_number, expire_month, expire_year, name_on_card) values (payments_seq.nextval, ?, sysdate, ?, ?, ?, ?)");

	for (int i=0; i<100000; i++) {
	    stmt.setDouble(1, 149.99);
	    stmt.setString(2, "1234567890123456");
	    stmt.setString(3, "12");
	    stmt.setString(4, "15");
	    stmt.setString(5, "Mr S ODonnell");
	    stmt.execute();
	    conn.commit();
	}
	
	long single_row_run_time  = System.currentTimeMillis() - start_of_single_row;
	System.out.println ("Single Row took: "+(single_row_run_time / 1000F)+" seconds");			  

	Thread.sleep(5000);

	long single_row_batches_start = System.currentTimeMillis();

	for (int i=0; i<100; i++) {
	    for (int j=0; j<1000; j++) {
		stmt.setDouble(1, 149.99);
		stmt.setString(2, "1234567890123456");
		stmt.setString(3, "12");
		stmt.setString(4, "15");
		stmt.setString(5, "Mr S ODonnell");
		stmt.execute();
	    }
	    conn.commit();
	}

	long single_row_batches_run_time = System.currentTimeMillis() - single_row_batches_start;
	System.out.println ("Single Row batches took: "+(single_row_batches_run_time / 1000F)+" seconds");
	
	Thread.sleep(5000);

	CallableStatement cstmt = 
            conn.prepareCall("{ call insert_payments_a(?, ?, ?, ?, ?, ? ) }"); 

	// JAVA arrays to hold the data.
        double[] payment_amount_array = new double[1000];
	String[] card_number_array    = new String[1000];
	String[] expire_month_array   = new String[1000];
	String[] expire_year_array    = new String[1000];
	String[] name_on_card_array   = new String[1000];

	int[] errors = new int[1001];
	
	long array_start = System.currentTimeMillis();

	for (int j=0; j<100; j++) {
	    // Fill the Java arrays.
	    for (int i=0; i< 1000; i++) {
		payment_amount_array[i]    = 99.99;
		card_number_array[i]       = "1234567890123456";
		expire_month_array[i]      = "12";
		expire_year_array[i]       = "15";
		name_on_card_array[i]     = "Mr S ODONNELL";
	    }

	    // Cast the Java arrays into Oracle arrays
	    ARRAY ora_payment_amount = new ARRAY (oracleNumberCollection,   conn, payment_amount_array);
	    ARRAY ora_card_number    = new ARRAY (oracleVarchar2Collection, conn, card_number_array);
	    ARRAY ora_expire_month   = new ARRAY (oracleVarchar2Collection, conn, expire_month_array);
	    ARRAY ora_expire_year    = new ARRAY (oracleVarchar2Collection, conn, expire_year_array);
	    ARRAY ora_name_on_card   = new ARRAY (oracleVarchar2Collection, conn, name_on_card_array);

	    // Bind the input arrays.
	    cstmt.setObject(1, ora_payment_amount);
	    cstmt.setObject(2, ora_card_number);
	    cstmt.setObject(3, ora_expire_month);
	    cstmt.setObject(4, ora_expire_year);
	    cstmt.setObject(5, ora_name_on_card);
	    
	    // Bind the output array, this will contain any exception indexes.
	    cstmt.registerOutParameter(6, OracleTypes.ARRAY, "INTEGER_T");
	    
	    cstmt.execute();
	
	    // Get any exceptions. Remember Oracle arrays index from 1,
	    // so all indexes are +1 off.

	    ARRAY ora_errors = ((OracleCallableStatement)cstmt).getARRAY(6);
	    conn.commit();
	}
	long array_run_time = System.currentTimeMillis() - array_start;
	System.out.println ("Array took: "+(array_run_time / 1000F)+" seconds");
	
    }
}
       