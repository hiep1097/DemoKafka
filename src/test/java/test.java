//import java.time.Duration;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Locale;
//import java.util.Map;
//import java.util.Queue;
//import java.util.concurrent.ConcurrentLinkedQueue;
//import java.util.concurrent.CopyOnWriteArrayList;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//
//import kafkareactor.SampleScenarios;
//import kafkareactor.SampleScenarios.*;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.TopicPartition;
//import org.junit.After;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import reactor.kafka.AbstractKafkaTest;
//import reactor.core.Disposable;
//import reactor.core.publisher.Mono;
//import reactor.kafka.receiver.ReceiverOptions;
//import reactor.kafka.receiver.ReceiverRecord;
//import reactor.kafka.receiver.KafkaReceiver;
//import reactor.kafka.receiver.ReceiverOffset;
//
//public class SampleScenariosTest extends AbstractKafkaTest {
//    private static final Logger log = LoggerFactory.getLogger(SampleScenariosTest.class.getName());
//
//    private List<Disposable> disposables = new ArrayList<>();
//
//    @After
//    public void tearDown() {
//        for (Disposable disposable : disposables)
//            disposable.dispose();
//    }
//
//    @Test
//    public void kafkaSink() throws Exception {
//        List<SampleScenarios.Person> expected = new ArrayList<>();
//        List<SampleScenarios.Person> received = new ArrayList<>();
//        subscribeToDestTopic("test-group", topic, received);
//        SampleScenarios.KafkaSink sink = new SampleScenarios.KafkaSink(bootstrapServers(), topic);
//        sink.source(createTestSource(10, expected));
//        sink.runScenario();
//        waitForMessages(expected, received);
//    }
//
//    @Test
//    public void kafkaSinkChain() throws Exception {
//        List<SampleScenarios.Person> expected = new ArrayList<>();
//        List<SampleScenarios.Person> received1 = new ArrayList<>();
//        List<SampleScenarios.Person> received2 = new ArrayList<>();
//        String topic2 = createNewTopic();
//        subscribeToDestTopic("test-group", topic, received1);
//        subscribeToDestTopic("test-group", topic2, received2);
//        SampleScenarios.KafkaSinkChain sinkChain = new SampleScenarios.KafkaSinkChain(bootstrapServers(), topic, topic2);
//        sinkChain.source(createTestSource(10, expected));
//        sinkChain.runScenario();
//        waitForMessages(expected, received1);
//        List<SampleScenarios.Person> expected2 = new ArrayList<>();
//        for (SampleScenarios.Person p : expected)
//            expected2.add(p.upperCase());
//        waitForMessages(expected2, received2);
//    }
//
//    @Test
//    public void kafkaSource() throws Exception {
//        List<SampleScenarios.Person> expected = new CopyOnWriteArrayList<>();
//        List<SampleScenarios.Person> received = new CopyOnWriteArrayList<>();
//        SampleScenarios.KafkaSource source = new SampleScenarios.KafkaSource(bootstrapServers(), topic) {
//            public Mono<Void> storeInDB(SampleScenarios.Person person) {
//                received.add(person);
//                return Mono.empty();
//            }
//            public ReceiverOptions<Integer, SampleScenarios.Person> receiverOptions() {
//                return super.receiverOptions().consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            }
//        };
//        disposables.add(source.flux().subscribe());
//        sendMessages(topic, 20, expected);
//        waitForMessages(expected, received);
//    }
//
//    @Test
//    public void kafkaTransform() throws Exception {
//        List<SampleScenarios.Person> expected = new ArrayList<>();
//        List<SampleScenarios.Person> received = new ArrayList<>();
//        String sourceTopic = topic;
//        String destTopic = createNewTopic();
//        SampleScenarios.KafkaTransform flow = new KafkaTransform(bootstrapServers(), sourceTopic, destTopic) {
//            public ReceiverOptions<Integer, Person> receiverOptions() {
//                return super.receiverOptions().consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            }
//        };
//        disposables.add(flow.flux().subscribe());
//        subscribeToDestTopic("test-group", destTopic, received);
//        sendMessages(sourceTopic, 20, expected);
//        for (Person p : expected)
//            p.email(flow.transform(p).value().email());
//        waitForMessages(expected, received);
//    }
//
//    @Test
//    public void atmostOnce() throws Exception {
//        List<Person> expected = new ArrayList<>();
//        List<Person> received = new ArrayList<>();
//        String sourceTopic = topic;
//        String destTopic = createNewTopic();
//        AtmostOnce flow = new AtmostOnce(bootstrapServers(), sourceTopic, destTopic) {
//            public ReceiverOptions<Integer, Person> receiverOptions() {
//                return super.receiverOptions().consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            }
//        };
//        disposables.add(flow.flux().subscribe());
//        subscribeToDestTopic("test-group", destTopic, received);
//        sendMessages(sourceTopic, 20, expected);
//        for (Person p : expected)
//            p.email(flow.transform(p).value().email());
//        waitForMessages(expected, received);
//    }
//
//    @Test
//    public void transactionalSend() throws Exception {
//        List<Person> expected1 = new ArrayList<>();
//        List<Person> received1 = new ArrayList<>();
//        List<Person> received2 = new ArrayList<>();
//        String destTopic1 = topic;
//        String destTopic2 = createNewTopic();
//        subscribeToDestTopic("test-group", destTopic1, received1);
//        subscribeToDestTopic("test-group", destTopic2, received2);
//        TransactionalSend sink = new TransactionalSend(bootstrapServers(), destTopic1, destTopic2);
//        sink.source(createTestSource(10, expected1));
//        for (Person p : expected1)
//            p.email(p.firstName().toLowerCase(Locale.ROOT) + "@kafka.io");
//        List<Person> expected2 = new ArrayList<>();
//        for (Person p : expected1) {
//            Person p2 = new Person(p.id(), p.firstName(), p.lastName());
//            p2.email(p.lastName().toLowerCase(Locale.ROOT) + "@reactor.io");
//            expected2.add(p2);
//        }
//        sink.runScenario();
//        waitForMessages(expected1, received1);
//        waitForMessages(expected2, received2);
//    }
//
//    @Test
//    public void exactlyOnce() throws Exception {
//        List<Person> expected = new ArrayList<>();
//        List<Person> received = new ArrayList<>();
//        String sourceTopic = topic;
//        String destTopic = createNewTopic();
//        KafkaExactlyOnce flow = new KafkaExactlyOnce(bootstrapServers(), sourceTopic, destTopic) {
//            public ReceiverOptions<Integer, Person> receiverOptions() {
//                return super.receiverOptions().consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            }
//        };
//        disposables.add(flow.flux().subscribe());
//        subscribeToDestTopic("test-group", destTopic, received);
//        sendMessages(sourceTopic, 500, expected);
//        for (Person p : expected)
//            p.email(flow.transform(p).value().email());
//        waitForMessages(expected, received);
//    }
//
//    @Test
//    public void exactlyOnceAbort() throws Exception {
//        int count = 60;
//        List<Person> expected = new ArrayList<>();
//        List<Person> received = new ArrayList<>();
//        AtomicInteger transformCounter = new AtomicInteger();
//        String sourceTopic = topic;
//        String destTopic = createNewTopic();
//        sendMessages(sourceTopic, count, expected);
//        KafkaExactlyOnce flow = new KafkaExactlyOnce(bootstrapServers(), sourceTopic, destTopic) {
//            public ReceiverOptions<Integer, Person> receiverOptions() {
//                return super.receiverOptions()
//                        .consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10")
//                        .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//                        .consumerProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
//            }
//            @Override
//            public ProducerRecord<Integer, Person> transform(Person p) {
//                if (transformCounter.incrementAndGet() == count / 2 + 3)
//                    throw new RuntimeException("Test exception");
//                return super.transform(p);
//            }
//        };
//        disposables.add(flow.flux().subscribe());
//        subscribeToDestTopic("test-group", destTopic, flow.receiverOptions(), received);
//        int expectedCount = count / 2;
//        TestUtils.waitUntil("Incorrect number of messages received, expected=" + expectedCount + ", received=",
//                () -> received.size(), r -> r.size() >= expectedCount, received, Duration.ofMillis(receiveTimeoutMillis));
//        assertEquals(expectedCount, received.size());
//    }
//
//    @Test
//    public void fanOut() throws Exception {
//        List<Person> expected1 = new ArrayList<>();
//        List<Person> expected2 = new ArrayList<>();
//        List<Person> received1 = new ArrayList<>();
//        List<Person> received2 = new ArrayList<>();
//        String sourceTopic = topic;
//        String destTopic1 = createNewTopic();
//        String destTopic2 = createNewTopic();
//        FanOut flow = new FanOut(bootstrapServers(), sourceTopic, destTopic1, destTopic2) {
//            public ReceiverOptions<Integer, Person> receiverOptions() {
//                return super.receiverOptions().consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            }
//        };
//        disposables.add(flow.flux().subscribe());
//        subscribeToDestTopic("group1", destTopic1, received1);
//        subscribeToDestTopic("group2", destTopic2, received2);
//        sendMessages(sourceTopic, 20, expected1);
//        for (Person p : expected1) {
//            Person p2 = new Person(p.id(), p.firstName(), p.lastName());
//            p2.email(flow.process2(p, false).value().email());
//            expected2.add(p2);
//            p.email(flow.process1(p, false).value().email());
//        }
//        waitForMessages(expected1, received1);
//        waitForMessages(expected2, received2);
//    }
//
//    @Test
//    public void partition() throws Exception {
//        List<Person> expected = new ArrayList<>();
//        Queue<Person> received = new ConcurrentLinkedQueue<Person>();
//        Map<Integer, List<Person>> partitionMap = new HashMap<>();
//        for (int i = 0; i < partitions; i++)
//            partitionMap.put(i, new ArrayList<>());
//        PartitionProcessor source = new PartitionProcessor(bootstrapServers(), topic) {
//            public ReceiverOptions<Integer, Person> receiverOptions() {
//                return super.receiverOptions().consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            }
//            @Override
//            public ReceiverOffset processRecord(TopicPartition topicPartition, ReceiverRecord<Integer, Person> message) {
//                Person person = message.value();
//                received.add(person);
//                partitionMap.get(message.partition()).add(person);
//                return super.processRecord(topicPartition, message);
//            }
//
//        };
//        disposables.add(source.flux().subscribe());
//        sendMessages(topic, 1000, expected);
//        waitForMessages(expected, received);
//        checkMessageOrder(partitionMap);
//    }
//
//    private void subscribeToDestTopic(String groupId, String topic, List<Person> received) {
//        KafkaSource source = new KafkaSource(bootstrapServers(), topic);
//        subscribeToDestTopic(groupId, topic, source.receiverOptions(), received);
//    }
//
//    private void subscribeToDestTopic(String groupId, String topic, ReceiverOptions<Integer, Person> receiverOptions, List<Person> received) {
//        receiverOptions = receiverOptions
//                .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//                .consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
//                .addAssignListener(partitions -> {
//                    log.debug("Group {} assigned {}", groupId, partitions);
//                    partitions.forEach(p -> log.trace("Group {} partition {} position {}", groupId, p, p.position()));
//                })
//                .addRevokeListener(p -> log.debug("Group {} revoked {}", groupId, p));
//        Disposable c = KafkaReceiver.create(receiverOptions.subscription(Collections.singleton(topic)))
//                .receive()
//                .subscribe(m -> {
//                    Person p = m.value();
//                    received.add(p);
//                    log.debug("Thread {} Received from {}: {} ", Thread.currentThread().getName(), m.topic(), p);
//                });
//        disposables.add(c);
//    }
//    private CommittableSource createTestSource(int count, List<Person> expected) {
//        for (int i = 0; i < count; i++)
//            expected.add(new Person(i, "foo" + i, "bar" + i));
//
//        return new CommittableSource(expected);
//    }
//    private void sendMessages(String topic, int count, List<Person> expected) throws Exception {
//        KafkaSink sink = new KafkaSink(bootstrapServers(), topic);
//        sink.source(createTestSource(count, expected));
//        sink.runScenario();
//    }
//    private void waitForMessages(Collection<Person> expected, Collection<Person> received) throws Exception {
//        try {
//            TestUtils.waitUntil("Incorrect number of messages received, expected=" + expected.size() + ", received=",
//                    () -> received.size(), r -> r.size() >= expected.size(), received, Duration.ofMillis(receiveTimeoutMillis));
//        } catch (Error e) {
//            TestUtils.printStackTrace(".*group.*");
//            throw e;
//        }
//        assertEquals(new HashSet<>(expected), new HashSet<>(received));
//    }
//    private void checkMessageOrder(Map<Integer, List<Person>> received) throws Exception {
//        for (List<Person> list : received.values()) {
//            for (int i = 1; i < list.size(); i++) {
//                assertTrue("Received out of order =: " + received, list.get(i - 1).id() < list.get(i).id());
//            }
//        }
//    }
//}