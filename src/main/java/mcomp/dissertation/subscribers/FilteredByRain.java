package mcomp.dissertation.subscribers;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.beans.LinkTrafficAndWeather;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPRuntime;

/**
 * 
 * This subscriber receives the archive sub-streams filtered on rain. Not
 * extending the {@link IntermediateSubscriber} because this proving to be the
 * bottle neck for high rate data streams and hence an array of aggregators are
 * supplied per filter.
 * 
 */
public class FilteredByRain {
   private Queue<LinkTrafficAndWeather> queue;
   private int count = 0;
   private EPRuntime[] cepRTAggregator;
   private static final Logger LOGGER = Logger.getLogger(FilteredByRain.class);

   /**
    * 
    * @param cepRTAggregator
    * @param queue
    */
   public FilteredByRain(final EPRuntime[] cepRTAggregator,
         final ConcurrentLinkedQueue<LinkTrafficAndWeather> queue) {
      this.queue = queue;
      this.cepRTAggregator = new EPRuntime[cepRTAggregator.length];
      for (int count = 0; count < cepRTAggregator.length; count++) {
         this.cepRTAggregator[count] = cepRTAggregator[count];

      }

      Thread thread = new Thread(new SendToNextOperator());
      thread.setDaemon(true);
      thread.start();
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

   private class SendToNextOperator implements Runnable {
      private int noOfAggregators;

      SendToNextOperator() {
         noOfAggregators = cepRTAggregator.length;
      }

      public void run() {
         while (true) {
            while (!queue.isEmpty()) {
               LinkTrafficAndWeather reading = queue.poll();
               long bucket = reading.getLinkId() % noOfAggregators;
               cepRTAggregator[(int) bucket].sendEvent(reading);
            }
         }

      }

   }

}
