package lab;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * 가벼운 consumer.
 *
 * - REBALANCE 로그를 stdout 에 찍어줍니다 (실험 2 에서 핵심)
 * - 각 메시지의 partition, offset, key, value 를 한 줄씩 출력
 * - Ctrl+C 로 종료
 */
public class SimpleConsumer {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: consume <topic> <group-id>");
            System.exit(1);
        }
        String topic = args[0];
        String group = args[1];

        String bootstrap = System.getenv().getOrDefault(
                "BOOTSTRAP",
                "localhost:9092,localhost:9094,localhost:9096");
        String offsetReset = System.getenv().getOrDefault("OFFSET_RESET", "earliest");
        String clientId = System.getenv().getOrDefault(
                "CLIENT_ID", "consumer-" + System.currentTimeMillis());

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        // Cooperative Sticky 는 발제 6장 "Cooperative Sticky Rebalancing" 에서 언급됨.
        // 학생이 직접 RangeAssignor 등으로 바꿔보는 것도 좋은 추가 실험.
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        System.out.printf("[consumer] bootstrap=%s topic=%s group=%s clientId=%s offsetReset=%s%n",
                bootstrap, topic, group, clientId, offsetReset);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    System.out.printf("REBALANCE [%s] revoked  = %s%n", clientId, partitions);
                }
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.printf("REBALANCE [%s] assigned = %s%n", clientId, partitions);
                }
            });

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("[consumer] shutting down...");
                consumer.wakeup();
            }));

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> r : records) {
                        System.out.printf("recv group=%s client=%s partition=%d offset=%d key=%s value=%s%n",
                                group, clientId, r.partition(), r.offset(), r.key(), r.value());
                    }
                }
            } catch (org.apache.kafka.common.errors.WakeupException e) {
                // expected on shutdown
            }
        }
    }
}
