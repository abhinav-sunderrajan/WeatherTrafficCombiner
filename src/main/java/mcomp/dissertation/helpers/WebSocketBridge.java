package mcomp.dissertation.helpers;

import java.net.InetSocketAddress;

import org.apache.log4j.Logger;
import org.java_websocket.WebSocketImpl;

/**
 * 
 * The interface which connects the java application to browser.
 * 
 */
public class WebSocketBridge {

   private StreamServer server;
   private static WebSocketBridge bridge;
   private static final Logger LOGGER = Logger.getLogger(WebSocketBridge.class);

   private WebSocketBridge(InetSocketAddress clientAddr) {
      server = new StreamServer(clientAddr);

   }

   /**
    * 
    * @param clientAddr
    * @return
    */
   public static WebSocketBridge getWebSocketServerInstance(
         InetSocketAddress clientAddr) {
      if (bridge == null) {
         bridge = new WebSocketBridge(clientAddr);
         bridge.startServer();
      }

      return bridge;

   }

   private void startServer() {
      try {
         WebSocketImpl.DEBUG = true;
         server.start();
         LOGGER.info("StreamServer started " + server.getAddress());
      } catch (Exception e) {
         e.printStackTrace();
      }

   }

   public void sendMessage(byte[] message) {
      server.sendToAll(message);

   }

   public void sendMessage(String message) {
      server.sendToAll(message);

   }

}
