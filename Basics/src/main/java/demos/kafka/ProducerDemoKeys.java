package demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka producer!");

        //create Producer Properties
        Properties properties = new Properties();
        //connect to localhost
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //connect to UpStash Playground
        properties.setProperty("bootstrap.servers", "vital-jawfish-9867-us1-kafka.upstash.io:9092");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"dml0YWwtamF3ZmlzaC05ODY3JN857th_J23Y6tc74OPDHU6dM0HZUTljXmgFyFs\" password=\"YTU0OWI0NzEtNDAxNi00NGUzLWJjYWEtMDE0NTUxMzlmNjJi\";");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j=0; j<2; j++) {
            for(int i=0; i<10; i++) {

                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello world " + i;

                //create a Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executed everytime a record is successfully sent or an exception is thrown
                        if(e == null) {
                            //record was successfully sent
                            log.info("Key: " + key + " | Partition: "+ recordMetadata.partition());
                        } else {
                            log.error("Error occurred while sending message", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //flush and close the producer
        producer.close();

    }
}
