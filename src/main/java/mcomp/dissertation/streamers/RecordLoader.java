package mcomp.dissertation.streamers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.beans.LinkTrafficAndWeather;

import org.apache.log4j.Logger;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

/**
 * This class is responsible for loading an optimal number of records to the
 * buffer for streaming.
 */
public class RecordLoader<T> extends AbstractLoader<T> {
   private Timestamp startTime;
   private boolean wakeFlag;
   private ConcurrentHashMap<Long, Coordinate> linkIdCoord;
   private Polygon polygon;
   private GeometryFactory gf;
   private boolean partionByLinkId;
   private static final Logger LOGGER = Logger.getLogger(RecordLoader.class);

   /**
    * 
    * @param buffer
    * @param startTime
    * @param connectionProperties
    * @param monitor
    * @param linkIdCoord
    * @param polygon
    * @param gf
    * @param partionByLinkId
    */
   public RecordLoader(final ConcurrentLinkedQueue<T> buffer,
         final long startTime, final Properties connectionProperties,
         final Object monitor,
         final ConcurrentHashMap<Long, Coordinate> linkIdCoord,
         final Polygon polygon, GeometryFactory gf, boolean partionByLinkId) {
      super(buffer, connectionProperties, monitor);
      this.startTime = new Timestamp(startTime);
      this.linkIdCoord = linkIdCoord;
      this.polygon = polygon;
      this.gf = gf;
      this.partionByLinkId = partionByLinkId;
      wakeFlag = true;

   }

   @SuppressWarnings("unchecked")
   public void run() {
      try {
         ResultSet rs = dbconnect.retrieveAtTimeStamp(startTime,
               partionByLinkId);
         Coordinate coord;
         Point point;
         long linkid;
         while (rs.next()) {
            linkid = rs.getLong(1);
            coord = linkIdCoord.get(linkid);
            point = gf.createPoint(coord);
            if (polygon.contains(point)) {
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

         }

         if (wakeFlag) {
            synchronized (monitor) {
               LOGGER.info("Wait for live streams before further database loading");
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
