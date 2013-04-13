package mcomp.dissertation.subscribers;

import mcomp.dissertation.beans.LinkTrafficAndWeather;
import mcomp.dissertation.helpers.EPLQueryRetrieve;

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

   /**
    * The join subscriber where the aggregated traffic/weather stream data is
    * sent to.
    * @param joiners
    * @param dbLoadRate
    */
   public FilteredByRain(LiveArchiveJoiner joiner, long dbLoadRate) {
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
      cepStatement = cepAdmAggregate.createEPL(helper
            .getAggregationQuery(dbLoadRate));
      cepStatement.setSubscriber(new AggregateSubscriber(joiner));

   }

   /**
    * Send for aggregation after filter.
    * @param reading
    */
   public void update(LinkTrafficAndWeather reading) {
      cepRTAggregate.sendEvent(reading);
   }
}
