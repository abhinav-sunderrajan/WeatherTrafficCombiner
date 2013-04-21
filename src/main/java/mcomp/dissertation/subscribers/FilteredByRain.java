package mcomp.dissertation.subscribers;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.beans.LinkTrafficAndWeather;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPRuntime;

/**
 * 
 * This subscriber receives the archive sub-streams filtered on rain.
 * 
 */
public class FilteredByRain extends
      IntermediateSubscriber<LinkTrafficAndWeather> {
   private Queue<LinkTrafficAndWeather> queue;
   private int count = 0;
   private static final Logger LOGGER = Logger.getLogger(FilteredByRain.class);

   /**
    * The join subscriber where the aggregated traffic/weather stream data is
    * sent to.
    * @param joiners
    * @param dbLoadRate
    * @param streamRate
    */
   public FilteredByRain(final EPRuntime cepRTLiveArchiveJoin,
         final ConcurrentLinkedQueue<LinkTrafficAndWeather> queue) {
      super(queue, cepRTLiveArchiveJoin);
      this.queue = queue;
   }

   /**
    * Send for aggregation after filter.
    * @param reading
    */
   public void update(LinkTrafficAndWeather reading) {
      queue.add(reading);
      count++;
      // if (count % 1000 == 0) {
      // LOGGER.info(reading.getLinkId() + " at " + reading.getTrafficTime()
      // + " and " + reading.getWeatherTime());
      // }
   }

}
