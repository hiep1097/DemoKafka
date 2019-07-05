import java.util.Properties;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerGroup {
    public static void main(String[] args) throws Exception {
        String topic = "topic1";
        String group = "KafkaExampleConsumer";
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Arrays.asList(topic));
        //print the topic name
        System.out.println("Subscribed to topic " + topic);
        final int giveUp = 100;   int noRecordsCount = 0;
        consumer.poll(0);
        consumer.seekToBeginning(consumer.assignment());
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            System.out.println(records.count());
            for (ConsumerRecord<String, String> record : records){
                //print the offset,key and value for the consumer records.
                System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
            }
            consumer.commitAsync();
        }
    }
}