package jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class CommitAndRollback {

  public static final String url = "jdbc:mysql://localhost:3306/mydb";
  public static final String user = "DCIJava";
  public static final String password = "pleasedontlook";
  private static Connection connection;
  private static Statement statement;

  public static void main( String[] args ) throws SQLException {
    commitOrRollback();
  }

  private static void commitOrRollback() {
    try {
      connection = DriverManager.getConnection( url, user, password );

      // change auto commit status
      connection.setAutoCommit( false );

      // execute update query
      // System.out.println( isCurrentActiveTransaction() );
      updateQuery();
      System.out.println( "Current active transaction: " + isCurrentActiveTransaction() );
      // commit
      connection.commit();
    } catch ( Exception e ) {
      try {
        // rollback
        System.out.println( e.getMessage() );
        connection.rollback();
      } catch ( SQLException e1 ) {
        e1.printStackTrace();
      }
    }
  }

  private static void updateQuery() throws Exception {

    String insertQuery = "INSERT INTO employees (id, name, age, department, salary) "
        + "SELECT 2, 'Alice', 21, 'IT', 2500 " + "FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM employees WHERE id = 2)";

    try {
      statement = connection.createStatement();

      // execute the insert query
      int rowsAffected = statement.executeUpdate( insertQuery );

      if ( rowsAffected > 0 ) {
        System.out.println( "Inserted: 2, 'Alice', 21, 'IT', 2500" );
      } else {
        System.out.println( "Employee with ID 2 already exists." );
      }

    } catch ( SQLException e ) {
      e.printStackTrace();
    }

  }

  private static boolean isCurrentActiveTransaction() throws SQLException {
    String sql = "SELECT COUNT(1) AS count FROM INFORMATION_SCHEMA.INNODB_TRX WHERE trx_mysql_thread_id = CONNECTION_ID()";

    try ( Statement statement = connection.createStatement(); ResultSet reSet = statement.executeQuery( sql ); ) {

      if ( reSet.next() ) {
        int count = reSet.getInt( "count" );
        return count > 0;
      }
    }

    return false;
  }

}
