package mcomp.dissertation.subscribers;

import java.sql.Timestamp;

import mcomp.dissertation.beans.LinkTrafficAndWeather;
import mcomp.dissertation.helpers.EPLQueryRetrieve;

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

   public LiveArchiveJoiner(long dbLoadRate) {
      cepConfigLiveArchiveJoin = new Configuration();
      cepConfigLiveArchiveJoin.getEngineDefaults().getThreading()
            .setListenerDispatchPreserveOrder(false);
      cepLiveArchiveJoin = EPServiceProviderManager.getProvider(
            "LIVEARCHIVEJOIN", cepConfigLiveArchiveJoin);
      cepConfigLiveArchiveJoin.addEventType("LINKWEATHERANDTRAFFIC",
            LinkTrafficAndWeather.class.getName());
      cepRTLiveArchiveJoin = cepLiveArchiveJoin.getEPRuntime();
      cepAdmLiveArchiveJoin = cepLiveArchiveJoin.getEPAdministrator();
      EPLQueryRetrieve.getHelperInstance();
      cepStatement = cepAdmLiveArchiveJoin.createEPL(EPLQueryRetrieve
            .getHelperInstance().getLiveArchiveCombineQuery(dbLoadRate));
      cepStatement.setSubscriber(FinalSubscriber.getFinalSubscriberInstance());

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
   public void update(long linkId, double speed, double volume,
         double temperature, double rain, Timestamp weatherTime,
         Timestamp trafficTime, long queryEvaltime) {
      LinkTrafficAndWeather reading = new LinkTrafficAndWeather();
      reading.setEvaltime(queryEvaltime);
      reading.setLinkId(linkId);
      reading.setRain(rain);
      reading.setSpeed(speed);
      reading.setTemperature(temperature);
      reading.setTrafficTime(trafficTime);
      reading.setWeatherTime(weatherTime);
      cepRTLiveArchiveJoin.sendEvent(reading);

   }

   /**
    * 
    * @return
    */
   public EPRuntime getEsperRunTime() {
      return cepRTLiveArchiveJoin;
   }

}
