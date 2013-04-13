package mcomp.dissertation.streamers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.beans.LinkTrafficAndWeather;

import org.apache.log4j.Logger;

/**
 * This class is responsible for loading an optimal number of records to the
 * buffer for streaming.
 */
public class RecordLoader<T> extends AbstractLoader<T> {
   private Timestamp startTime;
   private boolean wakeFlag;
   private static final Logger LOGGER = Logger.getLogger(RecordLoader.class);

   /**
    * 
    * @param buffer
    * @param startTime
    * @param connectionProperties
    * @param monitor
    */
   public RecordLoader(final ConcurrentLinkedQueue<T> buffer,
         final long startTime, final Properties connectionProperties,
         final Object monitor) {
      super(buffer, connectionProperties);
      this.startTime = new Timestamp(startTime);
      wakeFlag = true;

   }

   @SuppressWarnings("unchecked")
   public void run() {
      try {
         ResultSet rs = dbconnect.retrieveWithinTimeStamp(startTime);
         while (rs.next()) {
            LinkTrafficAndWeather bean = new LinkTrafficAndWeather();
            bean.setLinkId(rs.getLong(1));
            bean.setSpeed(rs.getFloat(2));
            bean.setVolume(rs.getInt(3));
            bean.setTemperature(rs.getDouble(4));
            bean.setRain(rs.getDouble(5));
            bean.setTrafficTime(rs.getTimestamp(6));
            bean.setWeatherTime(rs.getTimestamp(7));
            getBuffer().add((T) bean);
         }

         if (wakeFlag) {
            synchronized (monitor) {
               LOGGER.info("Wait for live streams before further databse loading");
               monitor.wait();
               LOGGER.info("Receiving live streams. Start database load normally");
            }
         }

         // Update the time stamps for the next fetch.
         long start = startTime.getTime() + REFRESH_INTERVAL;
         startTime = new Timestamp(start);
         wakeFlag = false;
      } catch (SQLException e) {
         LOGGER.error("Error accessing the database to retrieve archived data",
               e);
      } catch (InterruptedException e) {
         LOGGER.error("Error waking the sleeping threads", e);
      }

   }
}
