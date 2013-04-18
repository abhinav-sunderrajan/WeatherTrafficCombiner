package mcomp.dissertation.subscribers;

import java.sql.Timestamp;
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

public class LiveArchiveJoiner {
   private Configuration cepConfigLiveArchiveJoin;
   private EPServiceProvider cepLiveArchiveJoin;
   private EPRuntime cepRTLiveArchiveJoin;
   private EPAdministrator cepAdmLiveArchiveJoin;
   private EPStatement cepStatement;
   private int count = 0;
   private Queue<LinkTrafficAndWeather> queue;
   private static final Logger LOGGER = Logger
         .getLogger(LiveArchiveJoiner.class);

   public LiveArchiveJoiner(long dbLoadRate) {

      queue = new ConcurrentLinkedQueue<LinkTrafficAndWeather>();
      cepConfigLiveArchiveJoin = new Configuration();
      cepConfigLiveArchiveJoin.getEngineDefaults().getThreading()
            .setListenerDispatchPreserveOrder(false);
      cepLiveArchiveJoin = EPServiceProviderManager.getProvider(
            "LIVEARCHIVEJOIN_" + this.hashCode(), cepConfigLiveArchiveJoin);
      cepConfigLiveArchiveJoin.addEventType("LINKWEATHERANDTRAFFIC",
            LinkTrafficAndWeather.class.getName());
      cepRTLiveArchiveJoin = cepLiveArchiveJoin.getEPRuntime();
      cepAdmLiveArchiveJoin = cepLiveArchiveJoin.getEPAdministrator();
      EPLQueryRetrieve.getHelperInstance();
      cepStatement = cepAdmLiveArchiveJoin.createEPL(EPLQueryRetrieve
            .getHelperInstance().getLiveArchiveCombineQuery(dbLoadRate));
      cepStatement.setSubscriber(FinalSubscriber.getFinalSubscriberInstance());

      Thread thread = new Thread(new SendtoNextOperator());
      thread.setDaemon(true);
      thread.start();

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
      LinkTrafficAndWeather reading = new LinkTrafficAndWeather();
      reading.setEvaltime(queryEvaltime);
      reading.setLinkId(linkId);
      reading.setRain(rain);
      reading.setSpeed(speed);
      reading.setTemperature(temperature);
      reading.setTrafficTime(trafficTime);
      reading.setWeatherTime(weatherTime);
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

   private class SendtoNextOperator implements Runnable {

      public void run() {
         while (true) {
            while (!queue.isEmpty()) {
               cepRTLiveArchiveJoin.sendEvent(queue.poll());
            }

         }

      }

   }

}
