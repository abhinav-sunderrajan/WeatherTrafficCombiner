package mcomp.dissertation.subscribers;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.beans.LinkTrafficAndWeather;
import mcomp.dissertation.helpers.EPLQueryRetrieve;

import org.apache.log4j.Logger;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

public class FilteredByRain {
   private Configuration cepConfigAggregate;
   private EPServiceProvider cepAggregate;
   private EPRuntime cepRTAggregate;
   private EPAdministrator cepAdmAggregate;
   private EPLQueryRetrieve helper;
   private EPStatement cepStatement;
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
   public FilteredByRain(final LiveArchiveJoiner joiner, final long dbLoadRate,
         final int streamRate) {
      cepConfigAggregate = new Configuration();
      cepConfigAggregate.getEngineDefaults().getThreading()
            .setListenerDispatchPreserveOrder(false);
      cepAggregate = EPServiceProviderManager.getProvider("RAIN_CATEGORY_"
            + this.hashCode(), cepConfigAggregate);
      cepConfigAggregate.addEventType("LINKWEATHERANDTRAFFIC",
            LinkTrafficAndWeather.class.getName());
      cepRTAggregate = cepAggregate.getEPRuntime();
      cepAdmAggregate = cepAggregate.getEPAdministrator();
      helper = EPLQueryRetrieve.getHelperInstance();
      cepStatement = cepAdmAggregate.createEPL(helper.getAggregationQuery(
            dbLoadRate, streamRate));
      queue = new ConcurrentLinkedQueue<LinkTrafficAndWeather>();
      cepStatement.setSubscriber(new AggregateSubscriber(joiner));
      Thread thread = new Thread(new SendtoNextOperator());
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

   private class SendtoNextOperator implements Runnable {

      public void run() {
         while (true) {
            while (!queue.isEmpty()) {
               cepRTAggregate.sendEvent(queue.poll());
            }

         }

      }

   }
}
