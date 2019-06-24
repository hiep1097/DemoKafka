import java.util.Properties;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class VDConsumer {
    public static void main(String[] args) throws Exception {
        //Kafka consumer configuration settings
        String topicName = "topic1";
        Properties props = new Properties();

//        props.put("bootstrap.servers", "localhost:9092");
//        props.put("group.id", "test");
//        props.put("enable.auto.commit", "true");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("session.timeout.ms", "30000");
//        props.put("key.deserializer", StringDeserializer.class.getName());
//        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer
                <String, String>(props);

        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Arrays.asList(topicName));

        //print the topic name
        System.out.println("Subscribed to topic " + topicName);
        int i = 0;
        final int giveUp = 100;   int noRecordsCount = 0;
        consumer.poll(0);
        consumer.seekToBeginning(consumer.assignment());
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            if (records.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }
            System.out.println(records.count());
            for (ConsumerRecord<String, String> record : records){
                //print the offset,key and value for the consumer records.
                System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
            }
            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }
}