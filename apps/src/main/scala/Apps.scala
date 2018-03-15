import com.rebiekong.bdt.stream.kafka.worker.{PhoenixWriter, StreamLogWriter}
import com.rebiekong.bdt.stream.kafka.{Reader, StreamWorker}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Apps extends App {
  // config area
  val checkpoint = ""
  val phoenixURL = ""

  val kafkaServers = ""
  val topics = ""
  ////////////////////////////////

  val spark = SparkSession.builder()
    .appName("SparkStreamReader")
    //    .config("spark.sql.streaming.metricsEnabled", "true")
    .config("spark.sql.streaming.checkpointLocation", checkpoint)
    .getOrCreate()


  val t = Trigger.ProcessingTime(5000)
  Reader.builder
    .kafkaServers(kafkaServers)
    .subscribe(topics)
    .option("maxOffsetsPerTrigger", "1000")
    .addSink(StreamWorker.builder
    .foreach(new StreamLogWriter(phoenixURL)).setQueryName("common-log").setTrigger(t).build())
    .addSink(StreamWorker.builder
      .foreach(new PhoenixWriter(phoenixURL)).setQueryName("data-writer").setTrigger(t).build())
    .build().run(spark)

}
