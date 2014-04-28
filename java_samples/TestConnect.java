import java.sql.*;
import oracle.jdbc.*;

public class TestConnect {

    public static void main(String[] args)
        throws ClassNotFoundException, SQLException
    {
        DriverManager.registerDriver
            (new oracle.jdbc.driver.OracleDriver());
	String url = "jdbc:oracle:thin:@//10.13.20.31:1521/ptdbqa_qa";
        //            jdbc:oracle:thin:@//host:port/service


        Connection conn =
            DriverManager.getConnection(url,"sodonnell","sodonnell");

        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        ResultSet rset =
            stmt.executeQuery("select BANNER from SYS.V_$VERSION");
        while (rset.next()) {
            System.out.println (rset.getString(1));
        }
        stmt.close();
        System.out.println ("Ok.");
    }
}