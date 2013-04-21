package mcomp.dissertation.subscribers;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.beans.AggregatesPerLinkID;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPRuntime;

/**
 * 
 * Subscribes to the historical aggregates per link-Id when the rain fall is in
 * the same category.
 * 
 */
public class AggregateSubscriber extends
      IntermediateSubscriber<AggregatesPerLinkID> {

   private int count;
   private Queue<AggregatesPerLinkID> queue;
   private static final Logger LOGGER = Logger
         .getLogger(AggregateSubscriber.class);

   /**
    * 
    * @param cepRTAggregate
    * @param queue
    */
   public AggregateSubscriber(final EPRuntime cepRTAggregate,
         final ConcurrentLinkedQueue<AggregatesPerLinkID> queue) {
      super(queue, cepRTAggregate);
      this.queue = queue;
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
         if (count % 1000 == 0) {
            LOGGER.info(countRec + " records at " + bean.getLinkId() + " at "
                  + bean.getTrafficMinutes() + " and "
                  + bean.getWeatherMinutes());
         }

      }

   }

}
