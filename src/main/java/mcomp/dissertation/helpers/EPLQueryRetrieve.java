package mcomp.dissertation.helpers;

public class EPLQueryRetrieve {

   private static EPLQueryRetrieve helper;

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
   private EPLQueryRetrieve() {

   }

   /**
    * 
    * @return singleton instance.
    */
   public static EPLQueryRetrieve getHelperInstance() {
      if (helper == null) {
         helper = new EPLQueryRetrieve();
      }
      return helper;
   }

   public String[] getFilterArrayForLiveJoin() {
      String basic = "@Hint('reclaim_group_aged= 60')"
            + "select traffic.linkId as linkId,traffic.speed,traffic.volume,weather.temperature,weather.rain as rain, "
            + "weather.timeStamp as weatherTime, traffic.timeStamp as trafficTime,current_timestamp from mcomp.dissertation.beans.LiveTrafficBean"
            + ".std:unique(linkId,timeStamp.`hours`,timeStamp.`minutes`) as traffic left outer join "
            + "mcomp.dissertation.beans.LiveWeatherBean.std:unique(linkId,timeStamp.`hours`,timeStamp.`minutes`) as weather "
            + "on traffic.linkId=weather.linkId and traffic.timeStamp.`minutes`= weather.timeStamp.`minutes` "
            + "and traffic.timeStamp.`hours`= weather.timeStamp.`hours` where ";
      String[] filterArray = { basic + "weather.rain<2",
            basic + "weather.rain between 3 and 5",
            basic + "weather.rain between 6 and 8", basic + "weather.rain>8" };
      return filterArray;
   }

   public String[] getFilterArrayForArchive() {
      String basic = "select * from mcomp.dissertation.beans.LinkTrafficAndWeather as reading where ";
      String[] filterArray = { basic + "reading.rain<2",
            basic + "reading.rain between 3 and 5",
            basic + "reading.rain between 6 and 8", basic + "reading.rain>8" };
      return filterArray;
   }

   public String getAggregationQuery(long dbLoadRate) {
      String aggregationQuery;
      if (dbLoadRate < 50 && dbLoadRate >= 30) {
         dbLoadRate = 50;
      } else if (dbLoadRate < 30) {
         dbLoadRate = 40;
      }
      aggregationQuery = "@Hint('reclaim_group_aged="
            + dbLoadRate
            + ",') select linkId,avg(volume) as avgVolume, avg(speed) as avgSpeed,avg(rain) as avgRain, "
            + "avg(temperature) as avgtemp,trafficTime.`minutes` as minsTraffic,weatherTime.`minutes` as minsWeather "
            + ",trafficTime.`hours` as hrs from mcomp.dissertation.beans.LinkTrafficAndWeather"
            + ".std:groupwin(linkId,trafficTime.`minutes`,trafficTime.`hours`).win:time_length_batch("
            + (dbLoadRate * 10)
            + " msec,6) group by linkId,trafficTime.`minutes`,trafficTime.`hours`";
      return aggregationQuery;

   }

   public String getLiveArchiveCombineQuery(long dbLoadRate) {
      String liveArchiveJoin;
      long reclaimFrequency = 2 * dbLoadRate;
      if (dbLoadRate < 60) {
         dbLoadRate = 60;
         reclaimFrequency = 200;
      }
      liveArchiveJoin = "@Hint('reclaim_group_aged="
            + dbLoadRate
            + ", reclaim_group_freq="
            + reclaimFrequency
            + "') select live.linkId,live.speed,live.volume,live.rain,live.temperature,"
            + "historyAgg.linkId, historyAgg.averageSpeed,historyAgg.averageVolume,live.trafficTime, "
            + " historyAgg.averageRain,historyAgg.averageTemperature,live.evaltime from  mcomp.dissertation.beans.LinkTrafficAndWeather"
            + ".std:unique(linkId,trafficTime.`hours`,trafficTime.`minutes`) as live left outer join mcomp.dissertation"
            + ".beans.AggregatesPerLinkID.std:unique(linkId,`hours`,trafficMinutes) as historyAgg on historyAgg.linkId"
            + "=live.linkId and historyAgg.trafficMinutes=live.trafficTime.`minutes` and historyAgg.`hours`=live.trafficTime.`hours`";
      return liveArchiveJoin;
   }
}
