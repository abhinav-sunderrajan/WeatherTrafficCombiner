package mcomp.dissertation.streamers;

import java.text.DateFormat;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import mcomp.dissertation.helpers.NettyServer;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPRuntime;

public class GenericLiveStreamer<E> implements Runnable {
   private ScheduledExecutorService executor;
   private int count;
   private Object monitor;
   private Queue<E> buffer;
   private EPRuntime cepRTLiveArchive;
   private AtomicInteger streamRate;
   private int port;
   private static final Logger LOGGER = Logger
         .getLogger(GenericLiveStreamer.class);

   /**
    * 
    * @param buffer
    * @param cepRTLiveArchive
    * @param monitor
    * @param executor
    * @param streamRate
    * @param df
    */
   public GenericLiveStreamer(final ConcurrentLinkedQueue<E> buffer,
         final EPRuntime cepRTLiveArchive, final Object monitor,
         final ScheduledExecutorService executor,
         final AtomicInteger streamRate, final DateFormat df, final int port) {
      this.buffer = buffer;
      this.cepRTLiveArchive = cepRTLiveArchive;
      this.monitor = monitor;
      this.executor = executor;
      this.streamRate = streamRate;
      this.port = port;
      startListening();
      executor.scheduleAtFixedRate(new ThroughputMeasure(), 30, 30,
            TimeUnit.SECONDS);

   }

   private void startListening() {
      NettyServer<E> server = new NettyServer<E>(
            (ConcurrentLinkedQueue<E>) buffer);
      server.listen(port);
   }

   public void run() {
      while (buffer.isEmpty()) {
         // Poll till the producer has filled the queue. Bad approach will
         // optimize this.
      }
      synchronized (monitor) {
         monitor.notifyAll();
      }
      E obj = buffer.poll();
      cepRTLiveArchive.sendEvent(obj);
      count++;
   }

   public ScheduledFuture<?> startStreaming() {

      // Drive the live stream at the given rate specified.
      ScheduledFuture<?> liveFuture = executor.scheduleAtFixedRate(this, 0,
            (long) (streamRate.get()), TimeUnit.MICROSECONDS);
      return liveFuture;

   }

   private class ThroughputMeasure implements Runnable {
      int numOfMessages = 0;

      public void run() {
         int noOfMsgsin5sec = count - numOfMessages;
         numOfMessages = count;
         if (noOfMsgsin5sec == 0) {
            noOfMsgsin5sec = 1;
            LOGGER.info("No messages received in the past 30 seconds...");
            streamRate.compareAndSet(streamRate.get(), 30000000);
         } else {
            streamRate.compareAndSet(streamRate.get(),
                  30000000 / noOfMsgsin5sec);
            LOGGER.info("One message every " + 30000000 / noOfMsgsin5sec
                  + " microsecond");
         }

      }
   }

}
