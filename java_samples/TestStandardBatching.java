/*

To run this demo, you need to create a table as below:

create table test_java_array (
  c1 integer,
  c2 number,
  c3 varchar2(50)
);

This code uses standard JDBC batching

 */

import oracle.jdbc.*;
import java.sql.*;

public class TestStandardBatching {
    
    public static void main(String[] args)
	throws ClassNotFoundException, SQLException
    {
       DriverManager.registerDriver
           (new oracle.jdbc.OracleDriver());
	
        String url = "jdbc:oracle:thin:@//localhost:1521/local11gr2.world";
	
        Connection conn =
	    DriverManager.getConnection(url,"sodonnel","sodonnel");
	
        conn.setAutoCommit(false);

	PreparedStatement stmt = 
            conn.prepareStatement("insert into test_java_array (c1, c2, c3) values (?, ?, ?)");
	
	for (int i=0; i<100; i++) {
            stmt.setInt(1, 1);
            if (i == 50) {
		stmt.setDouble(2, 2000.123);	
	    } else {
		stmt.setDouble(2, 2.123);	
	    }
            stmt.setString(3, "abcdefghijk");
            stmt.addBatch();
	}
        int[] totalInserted = new int[100];
        try {
	    totalInserted = stmt.executeBatch();
        } catch(BatchUpdateException e) {
	    totalInserted = e.getUpdateCounts();
	    // handle bad record, and re-batch the remaining?
        }
        System.out.println (totalInserted.length);
	conn.commit();
        stmt.close();
    }
}

