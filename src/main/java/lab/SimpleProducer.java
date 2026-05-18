package lab;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 가벼운 producer.
 *
 * - acks, idempotence는 환경변수로 조절 (실험 3/4 에서 필요)
 * - 메시지 전송 결과(partition, offset)를 한 줄씩 stdout 으로 출력 → 실험 1에서 분포 관찰에 사용
 * - 키가 "__NULL__" 이면 null 키로 전송 (라운드 로빈)
 */
public class SimpleProducer {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: produce <topic> <key|__NULL__> <count> [sleep-ms]");
            System.exit(1);
        }
        String topic = args[0];
        String rawKey = args[1];
        int count = Integer.parseInt(args[2]);
        long sleepMs = args.length > 3 ? Long.parseLong(args[3]) : 100L;

        String key = "__NULL__".equals(rawKey) ? null : rawKey;

        String bootstrap = System.getenv().getOrDefault(
                "BOOTSTRAP",
                "localhost:9092,localhost:9094,localhost:9096");
        String acks = System.getenv().getOrDefault("ACKS", "all");
        boolean idempotence = Boolean.parseBoolean(
                System.getenv().getOrDefault("IDEMPOTENCE", "true"));
        String clientId = System.getenv().getOrDefault(
                "CLIENT_ID", "producer-" + System.currentTimeMillis());

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, acks);

        // idempotence=true 는 acks=all + retries>0 + max.in.flight<=5 가 필요합니다.
        // 실험 4에서 acks=0/1 로 바꾸려면 idempotence=false 도 함께 주어야 합니다.
        if (idempotence && "all".equals(acks)) {
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        } else {
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        }
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        System.out.printf("[producer] bootstrap=%s topic=%s key=%s count=%d acks=%s idempotence=%s%n",
                bootstrap, topic, key, count, acks, props.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                String value = String.format("msg-%d-%d", i, System.currentTimeMillis());
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                try {
                    RecordMetadata md = producer.send(record).get();
                    System.out.printf("sent topic=%s partition=%d offset=%d key=%s value=%s%n",
                            md.topic(), md.partition(), md.offset(), key, value);
                } catch (ExecutionException e) {
                    // 실험 4에서 의도적으로 발생: NotEnoughReplicasException 등
                    System.err.printf("FAILED i=%d key=%s cause=%s msg=%s%n",
                            i, key, e.getCause().getClass().getName(), e.getCause().getMessage());
                }
                if (sleepMs > 0) Thread.sleep(sleepMs);
            }
        }
    }
}
