package mcomp.dissertation.subscribers;

import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import mcomp.dissertation.display.StreamJoinDisplay;
import mcomp.dissertation.helpers.WebSocketBridge;

import org.apache.log4j.Logger;
import org.jfree.data.time.Minute;
import org.jfree.data.time.TimeSeries;
import org.json.simple.JSONObject;

import com.vividsolutions.jts.geom.Coordinate;

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
   private WebSocketBridge bridge;
   private Queue<JSONObject> queue;
   private static final Logger LOGGER = Logger.getLogger(FinalSubscriber.class);

   @SuppressWarnings("deprecation")
   private FinalSubscriber(InetSocketAddress subscriberAddress) {
      count = 0;
      this.df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss.SSS");
      display = StreamJoinDisplay.getInstance("Join Performance Measure");
      timer = new AtomicLong(0);
      // bridge = WebSocketBridge.getWebSocketServerInstance(subscriberAddress);
      queue = new ConcurrentLinkedQueue<JSONObject>();
      throughputFlag = true;
      display.addToDataSeries(
            new TimeSeries("Latency for Subscriber#" + this.hashCode()
                  + " in msec", Minute.class), (1 + this.hashCode()));
      display.addToDataSeries(new TimeSeries("Throughput/sec for Subscriber# "
            + this.hashCode(), Minute.class), (2 + this.hashCode()));
      valueMap = new HashMap<Integer, Double>();
      valueMap.put((2 + this.hashCode()), 0.0);
      valueMap.put((1 + this.hashCode()), 0.0);

      // Thread thread = new Thread(new SendToBrowser());
      // thread.setDaemon(true);
      // thread.start();
   }

   /**
    * There can be only one Final subscriber hence a singleton.
    * @param subscriberAddress the address of the subscriber of the joined live
    * and archive stream.
    * @return FinalSubscriber instance
    */
   public static FinalSubscriber getFinalSubscriberInstance(
         InetSocketAddress subscriberAddress) {
      if (subscriber == null) {
         subscriber = new FinalSubscriber(subscriberAddress);
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
    * @param latitude
    * @param longitude
    * @param coordinate
    */
   @SuppressWarnings("unchecked")
   public void update(final Long liveLinkId, final Double liveSpeed,
         final Double liveVolume, final Double liveRain,
         final Double liveTemperature, final Long archivelinkId,
         final Double archiveSpeed, final Double archiveVolume,
         final Timestamp liveTime, final Double archiveRain,
         final Double archiveTemperature, final long evalTime,
         final Coordinate coordinate) {
      count++;
      // JSONObject obj = new JSONObject();
      // obj.put("linkId", liveLinkId);
      // obj.put("rain", liveRain + "<-->" + archiveRain);
      // obj.put("temperature", liveTemperature + "<-->" + archiveTemperature);
      // obj.put("speed", liveSpeed + "<-->" + archiveSpeed);
      // obj.put("volume", liveVolume + "<-->" + archiveVolume);
      // obj.put("latitude", coordinate.x);
      // obj.put("longitude", coordinate.y);
      // obj.put("traffictime", liveTime);
      // queue.add(obj);
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

   private class SendToBrowser implements Runnable {

      public void run() {
         while (!queue.isEmpty()) {
            bridge.sendMessage(queue.poll().toJSONString());
         }

      }

   }

}
