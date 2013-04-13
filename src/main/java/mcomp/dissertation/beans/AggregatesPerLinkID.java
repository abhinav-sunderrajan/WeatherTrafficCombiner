package mcomp.dissertation.beans;

public class AggregatesPerLinkID {
   private long linkId;
   private int trafficMinutes;
   private int weatherMinutes;
   private int hours;
   private double averageSpeed;
   private double averageVolume;
   private double averageRain;
   private double averageTemperature;

   /**
    * @return the averageSpeed
    */
   public double getAverageSpeed() {
      return averageSpeed;
   }

   /**
    * @param averageSpeed the averageSpeed to set
    */
   public void setAverageSpeed(double averageSpeed) {
      this.averageSpeed = averageSpeed;
   }

   /**
    * @return the averageVolume
    */
   public double getAverageVolume() {
      return averageVolume;
   }

   /**
    * @param averageVolume the averageVolume to set
    */
   public void setAverageVolume(double averageVolume) {
      this.averageVolume = averageVolume;
   }

   /**
    * @return the averageRain
    */
   public double getAverageRain() {
      return averageRain;
   }

   /**
    * @param averageRain the averageRain to set
    */
   public void setAverageRain(double averageRain) {
      this.averageRain = averageRain;
   }

   /**
    * @return the averageTemperature
    */
   public double getAverageTemperature() {
      return averageTemperature;
   }

   /**
    * @param averageTemperature the averageTemperature to set
    */
   public void setAverageTemperature(double averageTemperature) {
      this.averageTemperature = averageTemperature;
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
    * @return the trafficMinutes
    */
   public int getTrafficMinutes() {
      return trafficMinutes;
   }

   /**
    * @param minutes the trafficMinutes to set
    */
   public void setTrafficMinutes(int minutes) {
      this.trafficMinutes = minutes;
   }

   /**
    * @return the hours
    */
   public int getHours() {
      return hours;
   }

   /**
    * @param hours the hours to set
    */
   public void setHours(int hours) {
      this.hours = hours;
   }

   public int getWeatherMinutes() {
      return weatherMinutes;
   }

   public void setWeatherMinutes(int weatherMinutes) {
      this.weatherMinutes = weatherMinutes;
   }

}
