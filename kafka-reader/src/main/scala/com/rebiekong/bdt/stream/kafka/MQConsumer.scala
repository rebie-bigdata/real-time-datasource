package com.rebiekong.bdt.stream.kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

class MQConsumer extends Thread {


  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "test")
  props.put("enable.auto.commit", "false")
  props.put("auto.commit.interval.ms", "1000")
  props.put("session.timeout.ms", "30000")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  val consumer = new KafkaConsumer[String, String](props)

  override def run(): Unit = super.run()
}
