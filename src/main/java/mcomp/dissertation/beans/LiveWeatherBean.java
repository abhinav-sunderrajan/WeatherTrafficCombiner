package mcomp.dissertation.beans;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Bean class representing the dummy weather link data.
 */
public class LiveWeatherBean implements Serializable {

   private long linkId;
   private double rain;
   private double temperature;
   private Timestamp timeStamp;
   private static final long serialVersionUID = 7618583543776349810L;

   /**
    * @return the linkId
    */
   public long getLinkId() {
      return linkId;
   }

   /**
    * @param linkId the linkId to set
    */
   public void setLinkId(long linkId) {
      this.linkId = linkId;
   }

   /**
    * @return the rain
    */
   public double getRain() {
      return rain;
   }

   /**
    * @param rain the rain to set
    */
   public void setRain(double rain) {
      this.rain = rain;
   }

   /**
    * @return the temperature
    */
   public double getTemperature() {
      return temperature;
   }

   /**
    * @param temperature the temperature to set
    */
   public void setTemperature(double temperature) {
      this.temperature = temperature;
   }

   /**
    * @return the timeStamp
    */
   public Timestamp getTimeStamp() {
      return timeStamp;
   }

   /**
    * @param timeStamp the timeStamp to set
    */
   public void setTimeStamp(Timestamp timeStamp) {
      this.timeStamp = timeStamp;
   }

}
