package mcomp.dissertation.subscribers;

import java.sql.Timestamp;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.beans.LinkTrafficAndWeather;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPRuntime;
import com.vividsolutions.jts.geom.Coordinate;

/**
 * 
 * This subscriber sends the live and the archive aggregated stream to be joined
 * by an Esper join operator instance.
 * 
 */
public class LiveArchiveJoiner extends
      IntermediateSubscriber<LinkTrafficAndWeather> {
   private Queue<LinkTrafficAndWeather> queue;
   private int count;
   private ConcurrentHashMap<Long, Coordinate> linkIdCoord;
   private EPRuntime cepRTLiveArchiveJoin;
   private static final Logger LOGGER = Logger
         .getLogger(LiveArchiveJoiner.class);

   /**
    * 
    * @param cepRTLiveArchiveJoin
    * @param queue
    * @param linkIdCoord
    */
   public LiveArchiveJoiner(final EPRuntime cepRTLiveArchiveJoin,
         final ConcurrentLinkedQueue<LinkTrafficAndWeather> queue,
         final ConcurrentHashMap<Long, Coordinate> linkIdCoord) {
      super(queue, cepRTLiveArchiveJoin);
      this.queue = queue;
      this.cepRTLiveArchiveJoin = cepRTLiveArchiveJoin;
      this.linkIdCoord = linkIdCoord;

   }

   /**
    * Send to be joined with the archive aggregated stream
    * @param linkId
    * @param speed
    * @param volume
    * @param temperature
    * @param rain
    * @param weatherTime
    * @param trafficTime
    * @param queryEvaltime
    */
   public void update(Long linkId, Double speed, Double volume,
         Double temperature, Double rain, Timestamp weatherTime,
         Timestamp trafficTime, Long queryEvaltime) {
      Coordinate coord = linkIdCoord.get(linkId);
      LinkTrafficAndWeather reading = new LinkTrafficAndWeather();
      reading.setEvaltime(queryEvaltime);
      reading.setLinkId(linkId);
      reading.setRain(rain);
      reading.setSpeed(speed);
      reading.setTemperature(temperature);
      reading.setTrafficTime(trafficTime);
      reading.setWeatherTime(weatherTime);
      reading.setLinkCoordinates(coord);
      queue.add(reading);
      count++;
      if (count % 1000 == 0) {
         LOGGER.info(linkId + " " + rain + "<-->" + speed + " at "
               + weatherTime + "<-->" + trafficTime);
      }

   }

   /**
    * 
    * @return
    */
   public EPRuntime getEsperRunTime() {
      return cepRTLiveArchiveJoin;
   }

}
