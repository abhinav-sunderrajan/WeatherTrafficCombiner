package mcomp.dissertation.subscribers;

import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.beans.LiveTrafficBean;

import com.espertech.esper.client.EPRuntime;

/**
 * 
 * Filtered on odd/even linkIds
 * 
 */
public class LinkIdTrafficFilter extends
      IntermediateSubscriber<LiveTrafficBean> {

   private ConcurrentLinkedQueue<LiveTrafficBean> queue;

   public LinkIdTrafficFilter(
         final ConcurrentLinkedQueue<LiveTrafficBean> queue,
         final EPRuntime cepRT) {
      super(queue, cepRT);
      this.queue = queue;

   }

   /**
    * send the linkId filtered event for join
    * @param event
    */
   public void update(LiveTrafficBean event) {
      queue.add(event);
   }

}
