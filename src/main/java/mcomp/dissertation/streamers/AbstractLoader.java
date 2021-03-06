package mcomp.dissertation.streamers;

import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.rdbmsaccess.DBConnect;

public abstract class AbstractLoader<T> implements Runnable {
   private Queue<T> buffer;
   protected DBConnect dbconnect;
   protected Object monitor;
   protected static final long REFRESH_INTERVAL = 300000;

   /**
    * 
    * @param buffer
    * @param connectionProperties
    * @param monitor
    */
   public AbstractLoader(final ConcurrentLinkedQueue<T> buffer,
         final Properties connectionProperties, final Object monitor) {
      this.buffer = buffer;
      this.monitor = monitor;
      dbconnect = new DBConnect();
      dbconnect.openDBConnection(connectionProperties);

   }

   public Queue<T> getBuffer() {
      return buffer;
   }

}
