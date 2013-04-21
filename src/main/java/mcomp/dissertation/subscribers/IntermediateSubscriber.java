package mcomp.dissertation.subscribers;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.espertech.esper.client.EPRuntime;

/**
 * 
 * This class represents the intermediate subscriber which passes the stream to
 * be processed by another Esper operator. Created to make the operators
 * asynchronous.
 * 
 * @param <E>
 */
public class IntermediateSubscriber<E> {

   private Queue<E> queue;
   private EPRuntime cepRT;

   /**
    * 
    * @param queue
    * @param cepRT
    */
   protected IntermediateSubscriber(final ConcurrentLinkedQueue<E> queue,
         final EPRuntime cepRT) {
      this.queue = queue;
      this.cepRT = cepRT;
      Thread thread = new Thread(new SendToNextOperator());
      thread.setDaemon(true);
      thread.start();

   }

   private class SendToNextOperator implements Runnable {

      public void run() {
         while (true) {
            while (!queue.isEmpty()) {
               cepRT.sendEvent(queue.poll());
            }
         }

      }

   }

}
