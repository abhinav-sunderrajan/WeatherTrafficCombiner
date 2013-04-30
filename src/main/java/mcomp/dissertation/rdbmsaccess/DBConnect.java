package mcomp.dissertation.rdbmsaccess;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;

import mcomp.dissertation.helpers.CommonHelper;

import org.apache.log4j.Logger;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;

/**
 * The class responsible for handling database operations.
 */
public class DBConnect {

   private Connection connect = null;
   private CommonHelper helper;
   private static final Logger LOGGER = Logger.getLogger(DBConnect.class);
   private boolean isEven;

   public DBConnect() {
      this.helper = CommonHelper.getHelperInstance();
   }

   /**
    * 
    * @param connectionProperties
    * @param streamOption
    * @return
    */
   public Connection openDBConnection(final Properties connectionProperties) {
      if (connectionProperties.getProperty("database.vendor").equalsIgnoreCase(
            "MySQL")) {

         String url = connectionProperties.getProperty("database.url");
         String dbName = connectionProperties.getProperty("database.name");
         String driver = "com.mysql.jdbc.Driver";
         String userName = connectionProperties
               .getProperty("database.username");
         String password = connectionProperties
               .getProperty("database.password");
         try {
            Class.forName(driver).newInstance();

            // Option returns many more rows in comparison to option 2
            // necessitating the below optimization.
            connect = (Connection) DriverManager.getConnection(url + dbName
                  + "?defaultFetchSize=10000&useCursorFetch=true", userName,
                  password);
            LOGGER.info("Connected to "
                  + connectionProperties.getProperty("database.vendor"));
            Runtime.getRuntime().addShutdownHook(new Hook());

         } catch (Exception e) {
            LOGGER.error(
                  "Unable to connect to database. Please check the settings", e);
         }

      }
      return connect;
   }

   /**
    * @param start
    * @param partitionByLinkId
    * @return ResultSet
    * @throws SQLException
    */
   @SuppressWarnings("deprecation")
   public ResultSet retrieveAtTimeStamp(final Timestamp start,
         boolean partitionByLinkId) throws SQLException {

      if (start.getMinutes() < 30) {
         isEven = true;
      } else {
         isEven = false;
      }
      ResultSet rs = null;
      PreparedStatement preparedStatement = helper.getDBAggregationQuery(
            partitionByLinkId, start, isEven, connect);
      try {
         preparedStatement.setTimestamp(1, start);
         rs = preparedStatement.executeQuery();
         LOGGER.info("Fetched records at time " + start);
      } catch (SQLException e) {
         LOGGER.error("Unable to retreive records", e);

      }
      return rs;
   }

   /**
    * 
    * Close the database connections that have been opened before terminating
    * program.
    * 
    */
   private class Hook extends Thread {

      public void run() {
         try {
            LOGGER.info("Closing the JDBC connection before shut down..");
            connect.close();
         } catch (SQLException e) {
            LOGGER.error("Error closing the JDBC connections..", e);
         }
      }
   }
}
