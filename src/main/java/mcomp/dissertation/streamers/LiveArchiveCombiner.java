package mcomp.dissertation.streamers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import mcomp.dissertation.beans.AggregatesPerLinkID;
import mcomp.dissertation.beans.LinkTrafficAndWeather;
import mcomp.dissertation.beans.LiveTrafficBean;
import mcomp.dissertation.beans.LiveWeatherBean;
import mcomp.dissertation.helpers.CommonHelper;
import mcomp.dissertation.subscribers.AggregateSubscriber;
import mcomp.dissertation.subscribers.FilteredByRain;
import mcomp.dissertation.subscribers.FinalSubscriber;
import mcomp.dissertation.subscribers.LinkIdTrafficFilter;
import mcomp.dissertation.subscribers.LinkIdWeatherFilter;
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
import com.espertech.esper.client.EPSubscriberException;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;

public class LiveArchiveCombiner {
   private long startTime;
   private EPServiceProvider cepLiveTrafficWeatherJoin;
   private EPAdministrator cepAdmLiveTrafficWeatherJoin;
   private Configuration cepConfigLiveTrafficWeatherJoin;
   private LiveArchiveJoiner[] joiners;
   private File file;
   private EPRuntime cepRTLiveTrafficWeatherJoin;
   private BufferedReader br;
   private static ConcurrentHashMap<Long, Coordinate> linkIdCoord;
   private static Polygon polygon;
   private static boolean partionByLinkId;
   private static ScheduledExecutorService executor;
   private static Properties connectionProperties;
   private static DateFormat df;
   private static AtomicInteger streamRate;
   private static long dbLoadRate;
   private static Properties configProperties;
   private static int numberOfArchiveStreams;
   private static int numberOfAggregatorsPerFilter;
   private static Object monitor;
   private static SAXReader reader;
   private static final String CONFIG_FILE_PATH = "src/main/resources/config.properties";
   private static final String CONNECTION_FILE_PATH = "src/main/resources/connection.properties";
   private static final String XML_FILE_PATH = "src/main/resources/livestreams.xml";
   private static final int ARCHIVE_STREAM_COUNT = 6;
   private static final int NUMBER_OF_CATEGORIES = 4;
   private static final Logger LOGGER = Logger
         .getLogger(LiveArchiveCombiner.class);
   private static final GeometryFactory gf = new GeometryFactory();;

   /**
    * @param configFilePath
    * @param connectionFilePath Instantiate all the required settings and start
    * the archive data stream threads.
    */
   private LiveArchiveCombiner(final String configFilePath,
         final String connectionFilePath) {
      try {
         LOGGER.info("Initializing all parameters");
         connectionProperties = new Properties();
         configProperties = new Properties();
         configProperties.load(new FileInputStream(configFilePath));
         monitor = new Object();
         connectionProperties.load(new FileInputStream(connectionFilePath));
         executor = Executors
               .newScheduledThreadPool(3 * numberOfArchiveStreams);
         streamRate = new AtomicInteger(Integer.parseInt(configProperties
               .getProperty("live.stream.rate.in.microsecs")));

         numberOfAggregatorsPerFilter = Integer.parseInt(configProperties
               .getProperty("number.of.aggregateoperators"));

         reader = new SAXReader();
         dbLoadRate = (long) (streamRate.get() * Float
               .parseFloat(configProperties.getProperty("db.prefetch.rate")));
         df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
         startTime = df.parse(
               configProperties.getProperty("archive.stream.start.time"))
               .getTime();
         partionByLinkId = Boolean.parseBoolean(configProperties
               .getProperty("partition.by.linkid"));

         CommonHelper helper = CommonHelper.getHelperInstance();

         // Link Id co=ordinate settings.
         LOGGER.info("Loading link ID coordinate information to memory");
         file = new File(configProperties.getProperty("linkid.cordinate.file"));
         br = new BufferedReader(new FileReader(file));
         linkIdCoord = new ConcurrentHashMap<Long, Coordinate>();
         helper.loadLinkIdCoordinates(linkIdCoord, file, br);
         LOGGER.info("Finished loading link ID coordinate information for "
               + linkIdCoord.size() + " link Ids to memory");

         // Create the polygon for spatial filter
         WKTReader reader = new WKTReader(gf);
         polygon = (Polygon) reader.read(configProperties
               .getProperty("spatial.polygon"));

         // Create and instantiate an array of subscribers which join the live
         // stream with the relevant and aggregated archive stream.
         joiners = new LiveArchiveJoiner[NUMBER_OF_CATEGORIES];
         for (int count = 0; count < NUMBER_OF_CATEGORIES; count++) {
            joiners[count] = new LiveArchiveJoiner(
                  createEsperEngineIntanceForLiveArchiveJoin(count),
                  new ConcurrentLinkedQueue<LinkTrafficAndWeather>(),
                  linkIdCoord);
         }

         // Instantiate the Esper parameters
         cepConfigLiveTrafficWeatherJoin = new Configuration();
         cepConfigLiveTrafficWeatherJoin.getEngineDefaults().getThreading()
               .setListenerDispatchPreserveOrder(false);
         cepLiveTrafficWeatherJoin = EPServiceProviderManager.getProvider(
               "LIVETRAFFICRAINJOINER", cepConfigLiveTrafficWeatherJoin);
         cepConfigLiveTrafficWeatherJoin.addEventType("TRAFFICBEAN",
               LiveTrafficBean.class.getName());
         cepConfigLiveTrafficWeatherJoin.addEventType("WEATHERBEAN",
               LiveWeatherBean.class.getName());
         cepRTLiveTrafficWeatherJoin = cepLiveTrafficWeatherJoin.getEPRuntime();
         cepAdmLiveTrafficWeatherJoin = cepLiveTrafficWeatherJoin
               .getEPAdministrator();
         String[] filters = helper.getFilterArrayForLiveJoin(dbLoadRate);
         cepAdmLiveTrafficWeatherJoin.getConfiguration().addVariable("cti",
               Long.class, 0);
         cepAdmLiveTrafficWeatherJoin.getConfiguration().addVariable(
               "trafficTimeMillis", Long.class, 0);
         cepAdmLiveTrafficWeatherJoin
               .createEPL("on mcomp.dissertation.beans.LiveTrafficBean as traffic set cti = traffic.linkId "
                     + ",trafficTimeMillis=traffic.timeStamp.time");
         for (int count = 0; count < filters.length; count++) {
            EPStatement cepStatement = cepAdmLiveTrafficWeatherJoin
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
      } catch (com.vividsolutions.jts.io.ParseException e) {
         LOGGER.error(
               "Error parsing the polygon string used for spatial filtering", e);
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
         SigarSystemMonitor sysMonitor = SigarSystemMonitor.getInstance(
               configProperties.getProperty("memory.file.dir"),
               streamRate.get(),
               configProperties.getProperty("image.save.directory"));
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
               EPRuntime cepRTLinkFilter = null;
               if (partionByLinkId) {
                  Configuration cepConfigLinkFilter = new Configuration();
                  cepConfigLinkFilter.getEngineDefaults().getThreading()
                        .setListenerDispatchPreserveOrder(false);
                  EPServiceProvider cepLinkFilter = EPServiceProviderManager
                        .getProvider("FILTER_TRAFFIC_BY_LINKID",
                              cepConfigLinkFilter);
                  cepConfigLinkFilter.addEventType("TRAFFICBEAN",
                        LiveTrafficBean.class.getName());
                  cepRTLinkFilter = cepLinkFilter.getEPRuntime();
                  EPAdministrator cepAdmLinkFilter = cepLinkFilter
                        .getEPAdministrator();
                  EPStatement cepStatement = cepAdmLinkFilter
                        .createEPL("select * from mcomp.dissertation.beans.LiveTrafficBean as traffic "
                              + " where traffic.linkId%2=cast(traffic.timeStamp.`minutes`/30,int)");
                  cepStatement.setSubscriber(new LinkIdTrafficFilter(
                        new ConcurrentLinkedQueue<LiveTrafficBean>(),
                        core.cepRTLiveTrafficWeatherJoin));

               }

               ConcurrentLinkedQueue<LiveTrafficBean> buffer = new ConcurrentLinkedQueue<LiveTrafficBean>();
               GenericLiveStreamer<LiveTrafficBean> streamer = new GenericLiveStreamer<LiveTrafficBean>(
                     buffer,
                     core.cepRTLiveTrafficWeatherJoin,
                     monitor,
                     executor,
                     streamRate,
                     df,
                     serverPort,
                     gf,
                     polygon,
                     linkIdCoord,
                     partionByLinkId,
                     "Ingestion rate in number of messages per second for traffic stream",
                     cepRTLinkFilter, configProperties
                           .getProperty("ingestion.file.dir.traffic"),
                     configProperties.getProperty("image.save.directory"));
               streamer.startStreaming();

            } else {
               EPRuntime cepRTLinkFilter = null;
               if (partionByLinkId) {
                  Configuration cepConfigLinkFilter = new Configuration();
                  cepConfigLinkFilter.getEngineDefaults().getThreading()
                        .setListenerDispatchPreserveOrder(false);
                  EPServiceProvider cepLinkFilter = EPServiceProviderManager
                        .getProvider("FILTER_WEATHER_BY_LINKID",
                              cepConfigLinkFilter);
                  cepConfigLinkFilter.addEventType("WEATHERBEAN",
                        LiveWeatherBean.class.getName());
                  cepRTLinkFilter = cepLinkFilter.getEPRuntime();
                  EPAdministrator cepAdmLinkFilter = cepLinkFilter
                        .getEPAdministrator();
                  EPStatement cepStatement = cepAdmLinkFilter
                        .createEPL("select * from mcomp.dissertation.beans.LiveWeatherBean as weather "
                              + " where weather.linkId%2=cast(weather.timeStamp.`minutes`/30,int)");
                  cepStatement.setSubscriber(new LinkIdWeatherFilter(
                        new ConcurrentLinkedQueue<LiveWeatherBean>(),
                        core.cepRTLiveTrafficWeatherJoin));

               }

               ConcurrentLinkedQueue<LiveWeatherBean> buffer = new ConcurrentLinkedQueue<LiveWeatherBean>();
               GenericLiveStreamer<LiveWeatherBean> streamer = new GenericLiveStreamer<LiveWeatherBean>(
                     buffer,
                     core.cepRTLiveTrafficWeatherJoin,
                     monitor,
                     executor,
                     streamRate,
                     df,
                     serverPort,
                     gf,
                     polygon,
                     linkIdCoord,
                     partionByLinkId,
                     "Ingestion rate in number of messages per second for weather stream",
                     cepRTLinkFilter, configProperties
                           .getProperty("ingestion.file.dir.weather"),
                     configProperties.getProperty("image.save.directory"));
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

      // Set up Esper engine instance to filter the archive data stream based on
      // the rain received at each link. Assigning an Esper filter instance for
      // each day of archive data in the hope of speeding up the process.

      Configuration[] cepConfigFilterArray = new Configuration[numberOfArchiveStreams];
      EPServiceProvider[] cepFilterArray = new EPServiceProvider[numberOfArchiveStreams];
      EPRuntime cepRTFilterArray[] = new EPRuntime[numberOfArchiveStreams];
      EPAdministrator[] cepAdmFilterArray = new EPAdministrator[numberOfArchiveStreams];
      ConcurrentLinkedQueue<LinkTrafficAndWeather>[] buffer = new ConcurrentLinkedQueue[numberOfArchiveStreams];

      CommonHelper helper = CommonHelper.getHelperInstance();
      String[] filters = helper.getFilterArrayForArchive();
      FilteredByRain[] filterSubscribers = new FilteredByRain[filters.length];
      for (int filterCount = 0; filterCount < filters.length; filterCount++) {
         filterSubscribers[filterCount] = new FilteredByRain(
               createEsperEngineIntanceForAggregation(filterCount),
               new ConcurrentLinkedQueue<LinkTrafficAndWeather>());
      }

      for (int count = 0; count < numberOfArchiveStreams; count++) {
         cepConfigFilterArray[count] = new Configuration();
         cepConfigFilterArray[count].getEngineDefaults().getThreading()
               .setListenerDispatchPreserveOrder(false);
         cepConfigFilterArray[count].addEventType("LINKWEATHERANDTRAFFIC",
               LinkTrafficAndWeather.class.getName());
         cepFilterArray[count] = EPServiceProviderManager
               .getProvider("FILTER_AND_GROUP_BY_RAIN_" + count,
                     cepConfigFilterArray[count]);
         cepRTFilterArray[count] = cepFilterArray[count].getEPRuntime();
         cepAdmFilterArray[count] = cepFilterArray[count].getEPAdministrator();
         // Filter by rain in the link id
         for (int filterCount = 0; filterCount < filters.length; filterCount++) {
            EPStatement cepStatement = cepAdmFilterArray[count]
                  .createEPL(filters[filterCount]);
            cepStatement.setSubscriber(filterSubscribers[filterCount]);
         }
         buffer[count] = new ConcurrentLinkedQueue<LinkTrafficAndWeather>();

         streamers[count] = new GenericArchiveStreamer<LinkTrafficAndWeather>(
               buffer[count], cepRTFilterArray[count], monitor, executor,
               streamRate, Float.parseFloat(configProperties
                     .getProperty("archive.stream.rate.param")));
         archiveStreamFutures[count] = streamers[count].startStreaming();

         loaders[count] = new RecordLoader<LinkTrafficAndWeather>(
               buffer[count], startTime, connectionProperties, monitor,
               linkIdCoord, polygon, gf, partionByLinkId);

         // retrieve records from the database for roughly every 30,000 records
         // from the live stream. This really depends upon the nature of the
         // live stream..
         dbLoadFutures[count] = executor.scheduleAtFixedRate(loaders[count], 0,
               dbLoadRate, TimeUnit.SECONDS);
         // Start the next archive stream for the records exactly a day after
         startTime = startTime + 24 * 3600 * 1000;
      }
   }

   /**
    * 
    * @param id
    * @return EPRuntime for joining the aggregated archive sub-streams and live
    * stream.
    * @throws UnknownHostException
    * @throws NumberFormatException
    * @throws EPSubscriberException
    */
   private EPRuntime createEsperEngineIntanceForLiveArchiveJoin(int id)
         throws EPSubscriberException, NumberFormatException,
         UnknownHostException {

      Configuration cepConfigLiveArchiveJoin = new Configuration();
      cepConfigLiveArchiveJoin.getEngineDefaults().getThreading()
            .setListenerDispatchPreserveOrder(false);
      EPServiceProvider cepLiveArchiveJoin = EPServiceProviderManager
            .getProvider("LIVEARCHIVEJOIN_" + id, cepConfigLiveArchiveJoin);
      cepConfigLiveArchiveJoin.addEventType("LINKWEATHERANDTRAFFIC",
            LinkTrafficAndWeather.class.getName());
      cepConfigLiveArchiveJoin.addEventType("AGGREGATESPERLINKID",
            AggregatesPerLinkID.class.getName());

      EPRuntime cepRTLiveArchiveJoin = cepLiveArchiveJoin.getEPRuntime();
      EPAdministrator cepAdmLiveArchiveJoin = cepLiveArchiveJoin
            .getEPAdministrator();
      CommonHelper.getHelperInstance();
      cepAdmLiveArchiveJoin.getConfiguration().addVariable("livehours",
            Integer.class, 0);
      cepAdmLiveArchiveJoin.getConfiguration().addVariable("livemins",
            Integer.class, 0);
      cepAdmLiveArchiveJoin
            .createEPL("on mcomp.dissertation.beans.LinkTrafficAndWeather as traffic "
                  + "set livehours = trafficTime.timeStamp.`hours`,livemins=trafficTime.timeStamp.`minutes`");
      EPStatement cepStatement = cepAdmLiveArchiveJoin.createEPL(CommonHelper
            .getHelperInstance().getLiveArchiveCombineQuery(dbLoadRate));
      cepStatement.setSubscriber(FinalSubscriber.getFinalSubscriberInstance(
            new InetSocketAddress(Integer.parseInt(configProperties
                  .getProperty("susbcriber.port"))), configProperties
                  .getProperty("throughput.file.dir"), streamRate.get(),
            configProperties.getProperty("image.save.directory")));
      return cepRTLiveArchiveJoin;

   }

   /**
    * Returns an array for aggregators per filter. This is necessary as always
    * since the aggregator operator has always been a bottle neck for high rate
    * data streams.
    * @param id
    * @return cepRTAggregateArray
    */
   private EPRuntime[] createEsperEngineIntanceForAggregation(int id) {

      Configuration cepConfigAggregate = new Configuration();
      cepConfigAggregate.getEngineDefaults().getThreading()
            .setListenerDispatchPreserveOrder(false);
      cepConfigAggregate.addEventType("LINKWEATHERANDTRAFFIC",
            LinkTrafficAndWeather.class.getName());

      EPServiceProvider[] cepAggregateArray = new EPServiceProvider[numberOfAggregatorsPerFilter];
      EPRuntime[] cepRTAggregateArray = new EPRuntime[numberOfAggregatorsPerFilter];
      EPAdministrator[] cepAdmAggregateArray = new EPAdministrator[numberOfAggregatorsPerFilter];

      for (int count = 0; count < numberOfAggregatorsPerFilter; count++) {
         cepAggregateArray[count] = EPServiceProviderManager.getProvider(
               "RAIN_CATEGORY_AGGREGATOR_" + id + "_" + count,
               cepConfigAggregate);
         cepRTAggregateArray[count] = cepAggregateArray[count].getEPRuntime();
         cepAdmAggregateArray[count] = cepAggregateArray[count]
               .getEPAdministrator();
         EPStatement cepStatement = cepAdmAggregateArray[count]
               .createEPL(CommonHelper.getHelperInstance().getAggregationQuery(
                     dbLoadRate,
                     (int) (streamRate.get() * Float
                           .parseFloat(configProperties
                                 .getProperty("archive.stream.rate.param")))));
         cepStatement.setSubscriber(new AggregateSubscriber(joiners[id]
               .getEsperRunTime(),
               new ConcurrentLinkedQueue<AggregatesPerLinkID>()));
      }

      return cepRTAggregateArray;

   }
}
