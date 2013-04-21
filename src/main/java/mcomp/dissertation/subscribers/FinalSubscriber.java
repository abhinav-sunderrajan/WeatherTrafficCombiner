package mcomp.dissertation.subscribers;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import mcomp.dissertation.display.StreamJoinDisplay;

import org.apache.log4j.Logger;
import org.jfree.data.time.Minute;
import org.jfree.data.time.TimeSeries;

/**
 * 
 * The final (hence not Extending {@link IntermediateSubscriber}) subscriber
 * which sends the data to the browser as JSON strings.
 * 
 */
public class FinalSubscriber {

   private static FinalSubscriber subscriber;
   private int count;
   private DateFormat df;
   private StreamJoinDisplay display;
   private long latency;
   private Map<Integer, Double> valueMap;
   private AtomicLong timer;
   private boolean throughputFlag;
   private static final Logger LOGGER = Logger.getLogger(FinalSubscriber.class);

   @SuppressWarnings("deprecation")
   private FinalSubscriber() {
      count = 0;
      this.df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss.SSS");
      display = StreamJoinDisplay.getInstance("Join Performance Measure");
      timer = new AtomicLong(0);
      throughputFlag = true;
      display.addToDataSeries(
            new TimeSeries("Latency for Subscriber#" + this.hashCode()
                  + " in msec", Minute.class), (1 + this.hashCode()));
      display.addToDataSeries(new TimeSeries("Throughput/sec for Subscriber# "
            + this.hashCode(), Minute.class), (2 + this.hashCode()));
      valueMap = new HashMap<Integer, Double>();
      valueMap.put((2 + this.hashCode()), 0.0);
      valueMap.put((1 + this.hashCode()), 0.0);
   }

   /**
    * There can be only one Final subscriber hence a singleton.
    * @return FinalSubscriber instance
    */
   public static FinalSubscriber getFinalSubscriberInstance() {
      if (subscriber == null) {
         subscriber = new FinalSubscriber();
      } else {
         return subscriber;
      }
      return subscriber;
   }

   /**
    * The method called when the final live and archive streams are joined.
    * @param liveLinkId
    * @param liveSpeed
    * @param liveVolume
    * @param liveRain
    * @param liveTemperature
    * @param archivelinkId
    * @param archiveSpeed
    * @param archiveVolume
    * @param liveTime
    * @param archiveRain
    * @param archiveTemperature
    * @param evalTime
    */
   public void update(Long liveLinkId, Double liveSpeed, Double liveVolume,
         Double liveRain, Double liveTemperature, Long archivelinkId,
         Double archiveSpeed, Double archiveVolume, Timestamp liveTime,
         Double archiveRain, Double archiveTemperature, long evalTime) {
      count++;
      if (throughputFlag) {
         timer.set(Calendar.getInstance().getTimeInMillis());
      }
      throughputFlag = false;
      if (count % 1000 == 0) {
         LOGGER.info("linkid(" + liveLinkId + "<-->" + archivelinkId
               + ") speed(" + liveSpeed + "<-->" + archiveSpeed + ") volume("
               + liveVolume + "<-->" + archiveVolume + " )rain(" + liveRain
               + "<-->" + archiveRain + ") temperature(" + liveTemperature
               + "<-->" + archiveTemperature + ") at time " + liveTime);
      }
      if (count % 1000 == 0) {
         double throughput = ((1000 * 1000) / (Calendar.getInstance()
               .getTimeInMillis() - timer.get()));
         latency = Calendar.getInstance().getTimeInMillis() - evalTime;
         valueMap.put((1 + this.hashCode()), latency / 1.0);
         valueMap.put((2 + this.hashCode()), throughput);
         display.refreshDisplayValues(valueMap);
         throughputFlag = true;
      }

   }

}
