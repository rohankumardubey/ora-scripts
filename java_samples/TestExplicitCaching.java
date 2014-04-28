import oracle.jdbc.*;
import java.sql.*;

public class TestExplicitCaching {
    
    public static void main(String[] args)
	throws ClassNotFoundException, SQLException
    {
       DriverManager.registerDriver
           (new oracle.jdbc.OracleDriver());
	
        String url = "jdbc:oracle:thin:@//localhost:1521/local11gr2.world";
	
        Connection conn =
	    DriverManager.getConnection(url,"sodonnel","sodonnel");
	
        conn.setAutoCommit(false);
	((OracleConnection)conn).setStatementCacheSize(100);
	((OracleConnection)conn).setExplicitCachingEnabled(true);	

        PreparedStatement stmt = null;

        stmt = ((OracleConnection)conn).getStatementWithKey("select1");
        if (stmt == null) {
	    System.out.println ("The query is not cached");
	    stmt = ((OracleConnection)conn).prepareStatement("select * from dual where 1 = ?");
	}
        stmt.setInt(1, 1);
        ResultSet rset = stmt.executeQuery();
	((OraclePreparedStatement)stmt).closeWithKey("select1");
        //
        // Run the same query again to prove it comes from the cache    
        //
        stmt = ((OracleConnection)conn).getStatementWithKey("select1");
        if (stmt == null) {
	    System.out.println ("The query is not cached");
	    stmt = ((OracleConnection)conn).prepareStatement("select * from dual where 1 = ?");
	} else {
            System.out.println ("The query came from the cache");
        }
        stmt.setInt(1, 1);
        rset = stmt.executeQuery();

        while (rset.next()) {
            System.out.println (rset.getString(1));
        }
	((OraclePreparedStatement)stmt).closeWithKey("select1");
    }
}

