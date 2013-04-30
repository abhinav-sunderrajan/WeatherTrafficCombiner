package mcomp.dissertation.streamers;

import java.text.DateFormat;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import mcomp.dissertation.helpers.NettyServer;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPRuntime;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

/**
 * 
 * Generic live streamer for both traffic and weather streams.
 * 
 * @param <E>
 */
public class GenericLiveStreamer<E> implements Runnable {
   private ScheduledExecutorService executor;
   private int count;
   private Object monitor;
   private Queue<E> buffer;
   private EPRuntime cepRTLiveArchive;
   private EPRuntime cepRTLinkFilter;
   private AtomicInteger streamRate;
   private int port;
   private boolean partionByLinkId;
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
    * @param partionByLinkId
    * @param linkIdCoord
    * @param polygon
    * @param gf
    * @param displayTitle
    * @param cepRTLinkFilter
    */
   public GenericLiveStreamer(final ConcurrentLinkedQueue<E> buffer,
         final EPRuntime cepRTLiveArchive, final Object monitor,
         final ScheduledExecutorService executor,
         final AtomicInteger streamRate, final DateFormat df, final int port,
         final GeometryFactory gf, final Polygon polygon,
         final ConcurrentHashMap<Long, Coordinate> linkIdCoord,
         final boolean partionByLinkId, final String displayTitle,
         EPRuntime cepRTLinkFilter) {
      this.buffer = buffer;
      this.cepRTLiveArchive = cepRTLiveArchive;
      this.cepRTLinkFilter = cepRTLinkFilter;
      this.monitor = monitor;
      this.partionByLinkId = partionByLinkId;
      this.executor = executor;
      this.streamRate = streamRate;
      this.port = port;
      startListening(linkIdCoord, polygon, gf, executor, streamRate,
            displayTitle);

   }

   private void startListening(ConcurrentHashMap<Long, Coordinate> linkIdCoord,
         Polygon polygon, GeometryFactory gf,
         ScheduledExecutorService executor, AtomicInteger streamRate,
         String displayTitle) {
      NettyServer<E> server = new NettyServer<E>(
            (ConcurrentLinkedQueue<E>) buffer, linkIdCoord, polygon, gf,
            executor, streamRate, displayTitle);
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

      // Send to be filtered if partionByLinkId is true
      if (partionByLinkId) {
         cepRTLinkFilter.sendEvent(obj);
      } else {
         cepRTLiveArchive.sendEvent(obj);
      }
      count++;
   }

   public ScheduledFuture<?> startStreaming() {

      // Drive the live stream at the given rate specified.
      ScheduledFuture<?> liveFuture = executor.scheduleAtFixedRate(this, 0,
            (long) (streamRate.get()), TimeUnit.MICROSECONDS);
      return liveFuture;

   }

}
