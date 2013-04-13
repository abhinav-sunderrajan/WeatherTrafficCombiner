package mcomp.dissertation.subscribers;

import mcomp.dissertation.beans.AggregatesPerLinkID;

public class AggregateSubscriber {

   /**
    * Send the aggregated stream to be joined with the live stream.
    * @param joiner
    */

   private LiveArchiveJoiner joiner;

   public AggregateSubscriber(LiveArchiveJoiner joiner) {
      this.joiner = joiner;
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
   public void update(final long linkId, double avgVolume, double avgSpeed,
         final double avgRain, final double avgTemp, final int trafficMins,
         final int weatherMins, final int hours) {

      AggregatesPerLinkID bean = new AggregatesPerLinkID();
      bean.setLinkId(hours);
      bean.setAverageRain(avgRain);
      bean.setAverageSpeed(avgSpeed);
      bean.setAverageVolume(avgVolume);
      bean.setHours(hours);
      bean.setTrafficMinutes(trafficMins);
      bean.setWeatherMinutes(weatherMins);
      joiner.getEsperRunTime().sendEvent(bean);

   }
}
