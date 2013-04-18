package mcomp.dissertation.subscribers;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.beans.AggregatesPerLinkID;

import org.apache.log4j.Logger;

public class AggregateSubscriber {

   private int count;
   private Queue<AggregatesPerLinkID> queue;
   private static final Logger LOGGER = Logger
         .getLogger(AggregateSubscriber.class);

   /**
    * Send the aggregated stream to be joined with the live stream.
    * @param joiner
    */

   private LiveArchiveJoiner joiner;

   public AggregateSubscriber(LiveArchiveJoiner joiner) {
      this.joiner = joiner;
      queue = new ConcurrentLinkedQueue<AggregatesPerLinkID>();
      Thread thread = new Thread(new SendtoNextOperator());
      thread.setDaemon(true);
      thread.start();
   }

   /**
    * @param linkId
    * @param trafficMins
    * @param weatherMins
    * @param hours
    * @param avgSpeed
    * @param avgVolume
    * @param avgRain
    * @param avgTemp
    */
   public void update(final Long countRec, final Long linkId, Double avgVolume,
         Double avgSpeed, final Double avgRain, final Double avgTemp,
         final Integer trafficMins, final Integer weatherMins,
         final Integer hours) {
      if (avgVolume != null) {
         AggregatesPerLinkID bean = new AggregatesPerLinkID();
         bean.setLinkId(linkId);
         bean.setAverageRain(avgRain);
         bean.setAverageTemperature(avgTemp);
         bean.setAverageSpeed(avgSpeed);
         bean.setAverageVolume(avgVolume);
         bean.setHours(hours);
         bean.setTrafficMinutes(trafficMins);
         bean.setWeatherMinutes(weatherMins);
         queue.add(bean);
         count++;
         // if (count % 1000 == 0) {
         // LOGGER.info(reading.getLinkId() + " at " + reading.getTrafficTime()
         // + " and " + reading.getWeatherTime());
         // }

      }

   }

   private class SendtoNextOperator implements Runnable {

      public void run() {
         while (true) {
            while (!queue.isEmpty()) {
               joiner.getEsperRunTime().sendEvent(queue.poll());
            }
         }

      }

   }
}
