package mcomp.dissertation.display;

@SuppressWarnings({ "serial" })
/**
 * 
 * To create a Singleton instance for checking parameters like latency and throughput of the application.
 *
 */
public class StreamJoinDisplay extends GenericChartDisplay {
   private static StreamJoinDisplay instance;

   /**
    * The title of the display
    * @param title
    */
   private StreamJoinDisplay(String title, String imageSaveDir) {
      super(title, imageSaveDir);

   }

   /**
    * 
    * @param title
    * @param imageSaveDir
    * @returns a singleton instance
    */
   public static StreamJoinDisplay getInstance(String title, String imageSaveDir) {
      if (instance == null) {
         instance = new StreamJoinDisplay(title, imageSaveDir);
      }
      return instance;

   }

}
