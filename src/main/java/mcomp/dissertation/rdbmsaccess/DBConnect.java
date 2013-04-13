package mcomp.dissertation.rdbmsaccess;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;

/**
 * The class responsible for handling database operations.
 */
public class DBConnect {

   private Connection connect = null;
   private static final Logger LOGGER = Logger.getLogger(DBConnect.class);

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

         } catch (Exception e) {
            LOGGER.error(
                  "Unable to connect to database. Please check the settings", e);
         }

      }
      return connect;
   }

   /**
    * @param start
    * @return ResultSet
    * @throws SQLException
    */
   public ResultSet retrieveWithinTimeStamp(final Timestamp start)
         throws SQLException {
      String SELECT_QUERY = "SELECT traffic.linkid,traffic.speed,traffic.volume,weather.temperature,weather.rain "
            + ",traffic.time_stamp,weather.time_stamp FROM dataarchive AS traffic INNER JOIN linkweather AS "
            + "weather ON traffic.time_stamp=?AND traffic.linkid=weather.linkid AND (traffic.time_stamp-INTERVAL "
            + (start.getMinutes() % 30) + " MINUTE)=weather.time_stamp";
      ResultSet rs = null;
      PreparedStatement preparedStatement = (PreparedStatement) connect
            .prepareStatement(SELECT_QUERY);
      preparedStatement.setFetchSize(Integer.MIN_VALUE);
      try {
         preparedStatement.setTimestamp(1, start);
         rs = preparedStatement.executeQuery();
         LOGGER.info("Fetched records at time " + start);
      } catch (SQLException e) {
         LOGGER.error("Unable to retreive records", e);

      }
      return rs;
   }
}
