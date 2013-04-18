package mcomp.dissertation.subscribers;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.espertech.esper.client.EPRuntime;

public class IntermediateSubscribers<E> {

   private Queue<E> queue;
   private EPRuntime cepRT;

   /**
    * 
    * @param queue
    * @param cepRT
    */
   protected IntermediateSubscribers(final ConcurrentLinkedQueue<E> queue,
         final EPRuntime cepRT) {
      this.queue = queue;
      this.cepRT = cepRT;
      Thread thread = new Thread(new SendToNextOperator());
      thread.setDaemon(true);
      thread.start();

   }

   private class SendToNextOperator implements Runnable {

      public void run() {
         while (queue.isEmpty()) {

         }
         cepRT.sendEvent(queue.poll());

      }

   }

}
