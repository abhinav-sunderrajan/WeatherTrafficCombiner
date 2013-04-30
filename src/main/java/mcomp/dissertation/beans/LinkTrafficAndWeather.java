package mcomp.dissertation.beans;

import java.sql.Timestamp;

import com.vividsolutions.jts.geom.Coordinate;

public class LinkTrafficAndWeather {
   private long linkId;
   private Timestamp trafficTime;
   private Timestamp weatherTime;
   private double rain;
   private double temperature;
   private double speed;
   private long evaltime;
   private double volume;
   private Coordinate linkCoordinates;

   /**
    * @return the evaltime
    */
   public long getEvaltime() {
      return evaltime;
   }

   /**
    * @param evaltime the evaluation time to set
    */
   public void setEvaltime(long evaltime) {
      this.evaltime = evaltime;
   }

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
    * @return the trafficTime
    */
   public Timestamp getTrafficTime() {
      return trafficTime;
   }

   /**
    * @param trafficTime the trafficTime to set
    */
   public void setTrafficTime(Timestamp trafficTime) {
      this.trafficTime = trafficTime;
   }

   /**
    * @return the weatherTime
    */
   public Timestamp getWeatherTime() {
      return weatherTime;
   }

   /**
    * @param weatherTime the weatherTime to set
    */
   public void setWeatherTime(Timestamp weatherTime) {
      this.weatherTime = weatherTime;
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
    * @return the speed
    */
   public double getSpeed() {
      return speed;
   }

   /**
    * @param speed the speed to set
    */
   public void setSpeed(double speed) {
      this.speed = speed;
   }

   /**
    * @return the volume
    */
   public double getVolume() {
      return volume;
   }

   /**
    * @param volume the volume to set
    */
   public void setVolume(double volume) {
      this.volume = volume;
   }

   /**
    * @return the linkCoordinates
    */
   public Coordinate getLinkCoordinates() {
      return linkCoordinates;
   }

   /**
    * @param linkCoordinates the linkCoordinates to set
    */
   public void setLinkCoordinates(Coordinate linkCoordinates) {
      this.linkCoordinates = linkCoordinates;
   }

}
