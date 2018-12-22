
import java.lang.Long
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KTable}

import scala.collection.JavaConverters.asJavaIterableConverter

object WordCountApp {
  def main(args:Array[String]): Unit = {
    println("Hello Kafka")
    val config: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      p.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      p.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      p
    }

    val builder = new KStreamBuilder
    val textLines: KStream[String, String] = builder.stream("streams-plaintext-input")
    val wordCounts: KTable[String, Long] = textLines
      .flatMapValues{textLine =>
        println(textLine.toLowerCase.split(" ").toIterable.asJava)
        textLine.toLowerCase.split(" ").toIterable.asJava}
      .groupBy((_, word) => word)
      .count("word-counts")

    wordCounts.to(Serdes.String(), Serdes.Long(), "streams-wordcount-output")

    val streams: KafkaStreams = new KafkaStreams(builder, config)
    streams.start()

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      streams.close(10, TimeUnit.SECONDS)
    }))
  }
}
