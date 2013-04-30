package mcomp.dissertation.helpers;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.vividsolutions.jts.geom.Coordinate;

public class CommonHelper {

   private static CommonHelper helper;
   private static final Logger LOGGER = Logger.getLogger(CommonHelper.class);

   /**
    * 
    * @param streamOption
    * @param dbLoadRate
    * @returns the query for joining the archive and the live data depending
    * upon the mode of operation.
    */

   /**
    * To prevent instantiation
    */
   private CommonHelper() {

   }

   /**
    * 
    * @return singleton instance.
    */
   public static CommonHelper getHelperInstance() {
      if (helper == null) {
         helper = new CommonHelper();
      }
      return helper;
   }

   public String[] getFilterArrayForLiveJoin(long dbLoadRate) {

      String basic = "select traffic.linkId as linkId,traffic.speed,traffic.volume,weather.temperature,weather.rain as rain, "
            + "weather.timeStamp as weatherTime, traffic.timeStamp as trafficTime,current_timestamp from mcomp.dissertation.beans.LiveTrafficBean"
            + " as traffic unidirectional left outer join mcomp.dissertation.beans.LiveWeatherBean"
            + ".std:unique(linkId,timeStamp.`minutes`,timeStamp.`hours`).win:expr(cti!=999999999 OR trafficTimeMillis!=(timeStamp.time + 30*60*1000))"
            + " as weather on  traffic.linkId=weather.linkId  and traffic.timeStamp.`hours`=weather.timeStamp.`hours`"
            + " where weather.timeStamp.`minutes`=(traffic.timeStamp.`minutes`-(traffic.timeStamp.`minutes`%30)) and ";
      String[] filterArray = { basic + "weather.rain<2",
            basic + "(weather.rain between 3 and 5)",
            basic + "(weather.rain between 6 and 8)", basic + "weather.rain>8" };
      return filterArray;
   }

   public String[] getFilterArrayForArchive() {
      String basic = "select * from mcomp.dissertation.beans.LinkTrafficAndWeather as reading where ";
      String[] filterArray = { basic + "reading.rain<2",
            basic + "reading.rain between 3 and 5",
            basic + "reading.rain between 6 and 8", basic + "reading.rain>8" };
      return filterArray;
   }

   /**
    * Wait for 500 more messages before time out while calculating the average.
    * Reclaim strategy is to remove those st:group windows that have not been
    * updated for half the DB load rate with a reclaim frequency of twice the DB
    * load rate.
    * @param dbLoadRate
    * @param archiveStreamRate
    * @return query to aggregate archive streams with similar rain.
    */
   public String getAggregationQuery(long dbLoadRate, int archiveStreamRate) {
      String aggregationQuery;
      long reclaimFrequency = dbLoadRate * 2;
      if (dbLoadRate < 60) {
         dbLoadRate = 60;
      }
      aggregationQuery = "@Hint('reclaim_group_aged="
            + (dbLoadRate / 2)
            + ", reclaim_group_freq="
            + reclaimFrequency
            + "') select count (*) as countrec, linkId,avg(volume) as avgVolume, avg(speed) as avgSpeed,avg(rain) as avgRain, "
            + "avg(temperature) as avgtemp,trafficTime.`minutes` as minsTraffic,weatherTime.`minutes` as minsWeather "
            + ",trafficTime.`hours` as hrs from mcomp.dissertation.beans.LinkTrafficAndWeather"
            + ".std:groupwin(linkId,trafficTime.`minutes`,trafficTime.`hours`).win:time_batch("
            + (archiveStreamRate / 3)
            + " msec) group by linkId,trafficTime.`minutes`,trafficTime.`hours`";
      return aggregationQuery;

   }

   public String getLiveArchiveCombineQuery(long dbLoadRate) {
      String liveArchiveJoin;
      long reclaimFrequency = 2 * dbLoadRate;
      if (dbLoadRate < 60) {
         dbLoadRate = 60;
         reclaimFrequency = 120;
      }
      liveArchiveJoin = "@Hint('reclaim_group_aged="
            + dbLoadRate
            + ", reclaim_group_freq="
            + reclaimFrequency
            + "') select live.linkId,live.speed,live.volume,live.rain,live.temperature,"
            + "historyAgg.linkId, historyAgg.averageSpeed,historyAgg.averageVolume,live.trafficTime, historyAgg.averageRain "
            + ",historyAgg.averageTemperature,live.evaltime,linkCoordinates from mcomp.dissertation.beans."
            + "LinkTrafficAndWeather as live unidirectional left outer join mcomp.dissertation"
            + ".beans.AggregatesPerLinkID.std:unique(linkId,`hours`,trafficMinutes) as historyAgg on historyAgg.linkId"
            + "=live.linkId and historyAgg.trafficMinutes=live.trafficTime.`minutes` and historyAgg.`hours`=live.trafficTime.`hours`";
      return liveArchiveJoin;
   }

   /**
    * 
    * @param linkIdCoord
    * @param file
    * @param br
    * @throws IOException
    */
   public void loadLinkIdCoordinates(
         ConcurrentHashMap<Long, Coordinate> linkIdCoord, File file,
         BufferedReader br) throws IOException {
      while (br.ready()) {
         String[] items = br.readLine().split(",");
         linkIdCoord.put(
               Long.parseLong(items[0]),
               new Coordinate(Double.parseDouble(items[1]), Double
                     .parseDouble(items[2])));

      }

      br.close();

   }

   /**
    * Return the prepared statement depending on whether the partition by linkID
    * mode of operation is chosen.
    * @return preparedStatement
    * @param partitionByLinkId
    * @param start
    * @param isEven
    * @param connect
    * @return
    * @throws SQLException
    */
   @SuppressWarnings("deprecation")
   public PreparedStatement getDBAggregationQuery(boolean partitionByLinkId,
         Timestamp start, boolean isEven, Connection connect)
         throws SQLException {
      String aggregateQuery = "";
      PreparedStatement preparedStatement = null;
      if (partitionByLinkId) {
         if (isEven) {
            aggregateQuery = "SELECT traffic.linkid,traffic.speed,traffic.volume,weather.temperature,weather.rain "
                  + ",traffic.time_stamp,weather.time_stamp FROM dataarchive AS traffic INNER JOIN linkweather AS "
                  + "weather ON traffic.time_stamp=? AND traffic.linkid%2=0 AND traffic.linkid=weather.linkid AND (traffic.time_stamp-INTERVAL "
                  + (start.getMinutes() % 30) + " MINUTE)=weather.time_stamp";

            LOGGER.info("Even numbered link IDS");
         } else {

            aggregateQuery = "SELECT traffic.linkid,traffic.speed,traffic.volume,weather.temperature,weather.rain "
                  + ",traffic.time_stamp,weather.time_stamp FROM dataarchive AS traffic INNER JOIN linkweather AS "
                  + "weather ON traffic.time_stamp=? AND traffic.linkid%2=1 AND traffic.linkid=weather.linkid AND (traffic.time_stamp-INTERVAL "
                  + (start.getMinutes() % 30) + " MINUTE)=weather.time_stamp";
            ;
            LOGGER.info("Odd numbered link IDS");
         }

      } else {
         aggregateQuery = "SELECT traffic.linkid,traffic.speed,traffic.volume,weather.temperature,weather.rain "
               + ",traffic.time_stamp,weather.time_stamp FROM dataarchive AS traffic INNER JOIN linkweather AS "
               + "weather ON traffic.time_stamp=?AND traffic.linkid=weather.linkid AND (traffic.time_stamp-INTERVAL "
               + (start.getMinutes() % 30) + " MINUTE)=weather.time_stamp";
      }
      preparedStatement = (PreparedStatement) connect
            .prepareStatement(aggregateQuery);
      preparedStatement.setFetchSize(Integer.MIN_VALUE);
      preparedStatement.setTimestamp(1, start);
      return preparedStatement;

   }
}
