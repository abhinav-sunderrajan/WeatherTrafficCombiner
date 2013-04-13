package mcomp.dissertation.streamers;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import mcomp.dissertation.beans.LinkTrafficAndWeather;
import mcomp.dissertation.beans.LiveTrafficBean;
import mcomp.dissertation.beans.LiveWeatherBean;
import mcomp.dissertation.helpers.EPLQueryRetrieve;
import mcomp.dissertation.subscribers.FilteredByRain;
import mcomp.dissertation.subscribers.LiveArchiveJoiner;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

public class LiveArchiveCombiner {
   private long startTime;
   private EPServiceProvider cepLiveArchive;
   private EPAdministrator cepAdmLiveArchive;
   private Configuration cepConfigLiveArchive;
   private EPRuntime cepRTLiveArchive;
   private LiveArchiveJoiner[] joiners;
   private static ScheduledExecutorService executor;
   private static Properties connectionProperties;
   private static DateFormat df;
   private static AtomicInteger streamRate;
   private static long dbLoadRate;
   private static Properties configProperties;
   private static int numberOfArchiveStreams;
   private static Object monitor;
   private static SAXReader reader;
   private static final String CONFIG_FILE_PATH = "src/main/resources/config.properties";
   private static final String CONNECTION_FILE_PATH = "src/main/resources/connection.properties";
   private static final String XML_FILE_PATH = "src/main/resources/livestreams.xml";
   private static final int ARCHIVE_STREAM_COUNT = 6;
   private static final int NUMBER_OF_CATEGORIES = 4;
   private static final Logger LOGGER = Logger
         .getLogger(LiveArchiveCombiner.class);

   /**
    * @param configFilePath
    * @param connectionFilePath Instantiate all the required settings and start
    * the archive data stream threads.
    */
   private LiveArchiveCombiner(final String configFilePath,
         final String connectionFilePath) {
      try {

         connectionProperties = new Properties();
         configProperties = new Properties();
         configProperties.load(new FileInputStream(configFilePath));
         monitor = new Object();
         connectionProperties.load(new FileInputStream(connectionFilePath));
         executor = Executors
               .newScheduledThreadPool(3 * numberOfArchiveStreams);
         streamRate = new AtomicInteger(Integer.parseInt(configProperties
               .getProperty("live.stream.rate.in.microsecs")));

         // Create and instantiate an array of subscribers which join the live
         // stream with the relevant and aggregated archive stream.
         joiners = new LiveArchiveJoiner[NUMBER_OF_CATEGORIES];
         for (int count = 0; count < NUMBER_OF_CATEGORIES; count++) {
            joiners[count] = new LiveArchiveJoiner(dbLoadRate);
         }

         reader = new SAXReader();
         dbLoadRate = (long) (streamRate.get() * Float
               .parseFloat(configProperties.getProperty("db.prefetch.rate")));
         df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
         startTime = df.parse(
               configProperties.getProperty("archive.stream.start.time"))
               .getTime();

         // Instantiate the Esper parameter arrays
         cepConfigLiveArchive = new Configuration();
         cepConfigLiveArchive.getEngineDefaults().getThreading()
               .setListenerDispatchPreserveOrder(false);
         cepLiveArchive = EPServiceProviderManager.getProvider(
               "LIVETRAFFICRAINJOINER", cepConfigLiveArchive);
         cepConfigLiveArchive.addEventType("TRAFFICBEAN",
               LiveTrafficBean.class.getName());
         cepConfigLiveArchive.addEventType("WEATHERBEAN",
               LiveWeatherBean.class.getName());
         cepRTLiveArchive = cepLiveArchive.getEPRuntime();
         cepAdmLiveArchive = cepLiveArchive.getEPAdministrator();
         EPLQueryRetrieve helper = EPLQueryRetrieve.getHelperInstance();
         String[] filters = helper.getFilterArrayForLiveJoin();
         for (int count = 0; count < filters.length; count++) {
            EPStatement cepStatement = cepAdmLiveArchive
                  .createEPL(filters[count]);
            cepStatement.setSubscriber(joiners[count]);
         }

         // End of Esper configuration for the join

      } catch (ParseException e) {
         LOGGER.error(
               "Unable to determine the start date/stream rate from config file. Please check it",
               e);

      } catch (FileNotFoundException e) {
         LOGGER.error("Unable to find the config/connection properties files",
               e);
      } catch (IOException e) {
         LOGGER.error("Properties file contains non unicode values ", e);
      }

   }

   /**
    * @param args
    */
   @SuppressWarnings("unchecked")
   public static void main(final String[] args) {

      String configFilePath;
      String connectionFilePath;
      String xmlFilePath;
      if (args.length < 4) {
         configFilePath = CONFIG_FILE_PATH;
         connectionFilePath = CONNECTION_FILE_PATH;
         numberOfArchiveStreams = ARCHIVE_STREAM_COUNT;
         xmlFilePath = XML_FILE_PATH;

      } else {
         configFilePath = args[0];
         connectionFilePath = args[1];
         numberOfArchiveStreams = Integer.parseInt(args[2]);
         xmlFilePath = args[3];

      }
      try {
         LiveArchiveCombiner core;
         core = new LiveArchiveCombiner(configFilePath, connectionFilePath);

         // Start monitoring the system CPU, memory parameters
         SigarSystemMonitor sysMonitor = SigarSystemMonitor.getInstance();
         sysMonitor.setCpuUsageScalefactor((Double.parseDouble(configProperties
               .getProperty("cpu.usage.scale.factor"))));
         executor.scheduleAtFixedRate(sysMonitor, 0, 60, TimeUnit.SECONDS);

         core.setUpArchiveStreams();

         // Start streaming the live data.
         reader = new SAXReader();
         InputStream streamxml = new FileInputStream(xmlFilePath);
         reader = new SAXReader();
         Document doc = reader.read(streamxml);
         Element docRoot = doc.getRootElement();
         List<Element> streams = docRoot.elements();
         for (Element stream : streams) {
            int serverPort = Integer.parseInt(stream.attribute(1).getText());
            String streamName = stream.attribute(0).getText();
            if (streamName.equalsIgnoreCase("traffic")) {
               ConcurrentLinkedQueue<LiveTrafficBean> buffer = new ConcurrentLinkedQueue<LiveTrafficBean>();
               GenericLiveStreamer<LiveTrafficBean> streamer = new GenericLiveStreamer<LiveTrafficBean>(
                     buffer, core.cepRTLiveArchive, monitor, executor,
                     streamRate, df, serverPort);
               streamer.startStreaming();

            } else {
               ConcurrentLinkedQueue<LiveWeatherBean> buffer = new ConcurrentLinkedQueue<LiveWeatherBean>();
               GenericLiveStreamer<LiveWeatherBean> streamer = new GenericLiveStreamer<LiveWeatherBean>(
                     buffer, core.cepRTLiveArchive, monitor, executor,
                     streamRate, df, serverPort);
               streamer.startStreaming();

            }

         }

      } catch (FileNotFoundException e) {
         LOGGER.error("Unable to find xml file containing stream info", e);
         e.printStackTrace();
      } catch (DocumentException e) {
         LOGGER.error("Erroneous stream info xml file. Please check", e);
      }

   }

   @SuppressWarnings("unchecked")
   private void setUpArchiveStreams() {
      // Initialize the local variables
      GenericArchiveStreamer<?>[] streamers = new GenericArchiveStreamer[numberOfArchiveStreams];
      ScheduledFuture<?>[] archiveStreamFutures = new ScheduledFuture[numberOfArchiveStreams];
      AbstractLoader<LinkTrafficAndWeather>[] loaders = new AbstractLoader[numberOfArchiveStreams];
      ScheduledFuture<?>[] dbLoadFutures = new ScheduledFuture[numberOfArchiveStreams];

      Configuration cepConfigFilter = new Configuration();
      cepConfigFilter.getEngineDefaults().getThreading()
            .setListenerDispatchPreserveOrder(false);
      EPServiceProvider cepFilter = EPServiceProviderManager.getProvider(
            "GROUPBYRAIN", cepConfigFilter);
      cepConfigFilter.addEventType("LINKWEATHERANDTRAFFIC",
            LinkTrafficAndWeather.class.getName());
      EPRuntime cepRTFilter = cepFilter.getEPRuntime();
      EPAdministrator cepAdmFilter = cepFilter.getEPAdministrator();
      EPLQueryRetrieve helper = EPLQueryRetrieve.getHelperInstance();
      String[] filters = helper.getFilterArrayForArchive();

      // Filter by rain in the link id
      for (int count = 0; count < filters.length; count++) {
         EPStatement cepStatement = cepAdmFilter.createEPL(filters[count]);
         cepStatement.setSubscriber(new FilteredByRain(joiners[count],
               dbLoadRate));
      }

      ConcurrentLinkedQueue<LinkTrafficAndWeather>[] buffer = new ConcurrentLinkedQueue[numberOfArchiveStreams];

      // Create a shared buffer between the thread retrieving records from
      // the database and the the thread streaming those records.
      for (int count = 0; count < numberOfArchiveStreams; count++) {
         buffer[count] = new ConcurrentLinkedQueue<LinkTrafficAndWeather>();
         streamers[count] = new GenericArchiveStreamer<LinkTrafficAndWeather>(
               buffer[count], cepRTFilter, monitor, executor, streamRate,
               Float.parseFloat(configProperties
                     .getProperty("archive.stream.rate.param")));
         archiveStreamFutures[count] = streamers[count].startStreaming();
         loaders[count] = new RecordLoader<LinkTrafficAndWeather>(
               buffer[count], startTime, connectionProperties, monitor);

         // retrieve records from the database for every 30,000 records from the
         // live stream. This really depends upon the nature of the live
         // stream..
         dbLoadFutures[count] = executor.scheduleAtFixedRate(loaders[count], 0,
               dbLoadRate, TimeUnit.SECONDS);
         // Start the next archive stream for the records exactly a day after
         startTime = startTime + 24 * 3600 * 1000;
      }
   }
}
