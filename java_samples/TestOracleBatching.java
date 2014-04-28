/*

To run this demo, you need to create a table as below:

create table test_java_array (
  c1 integer,
  c2 number,
  c3 varchar2(50)
);

This code uses Oracle batching

*/

import oracle.jdbc.*;
import java.sql.*;

public class TestOracleBatching {
    
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

	((OraclePreparedStatement)stmt).setExecuteBatch(100);
	
	int[] totalInserted = new int[100];
	for (int i=0; i<100; i++) {
            stmt.setInt(1, 1);
            if (i == 50) {
		stmt.setDouble(2, 2000.123);	
	    } else {
		stmt.setDouble(2, 2.123);	
	    }
            stmt.setString(3, "abcdefghijk");

	    try {
	        stmt.execute();
	    } catch(BatchUpdateException e) {
	    // } catch(SQLException e) { FOR ojdbc14
		System.out.println ("in exception!");
		totalInserted = e.getUpdateCounts();
	    }
	}
	System.out.println ("Row count from the statement handle:");
	System.out.println (stmt.getUpdateCount());
	System.out.println ("Row count from exception handler:");
	System.out.println (totalInserted.length);
	System.out.println (totalInserted[49]);
	conn.commit();
        stmt.close();
    }
}

