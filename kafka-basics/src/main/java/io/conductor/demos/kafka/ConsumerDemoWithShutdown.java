package io.conductor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");

        String bootstrapServer = "127.0.0.1:9092";
        String groupID = "my-second-application";
        String topic = "demo_java";

        // create Consumer Configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


        // get reference to current thread
        final Thread mainThread = Thread.currentThread();

        // add shutdown hook
          Runtime.getRuntime().addShutdownHook(new Thread() {
              @Override
              public void run() {
                  log.info("Detecting a shutdown, let's exit by calling consumer.wakeup()...");
                  consumer.wakeup(); //called so that consumer.poll will leave the while infinite loop

                  //join the main thread to allow the execution of the code in the main thread
                  try{
                      mainThread.join();
                  } catch (InterruptedException e){
                      e.printStackTrace();
                  }
              }
          });

          try {

              // subscribe to topics
              consumer.subscribe(Collections.singleton(topic));

              // poll for new data
              while(true) {
                  log.info("Polling");

                  ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                  for (ConsumerRecord<String, String> record : records) {
                      log.info("Key: " + record.key() + ", Value: " + record.value());
                      log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                  }
              }
          } catch (WakeupException e){
              log.info("Wake up exception!");
              // we ignore this as this is an expected exception when closing a consumer
          } catch (Exception e){
              log.error("Unexpected exception");
          } finally {
              consumer.close();
              log.error("The consumer is now closed");
          }
    }
}
