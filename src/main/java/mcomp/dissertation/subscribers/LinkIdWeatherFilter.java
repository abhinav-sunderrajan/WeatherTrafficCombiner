package mcomp.dissertation.subscribers;

import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.beans.LiveWeatherBean;

import com.espertech.esper.client.EPRuntime;

/**
 * 
 * Filtered on odd/even linkIds
 * 
 */
public class LinkIdWeatherFilter extends
      IntermediateSubscriber<LiveWeatherBean> {

   private ConcurrentLinkedQueue<LiveWeatherBean> queue;

   public LinkIdWeatherFilter(
         final ConcurrentLinkedQueue<LiveWeatherBean> queue,
         final EPRuntime cepRT) {
      super(queue, cepRT);
      this.queue = queue;
   }

   /**
    * send the linkId filtered event for join
    * @param event
    */
   public void update(LiveWeatherBean event) {
      queue.add(event);

   }

}
