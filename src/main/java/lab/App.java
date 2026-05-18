package lab;

/**
 * CLI dispatcher.
 *
 * 사용법:
 *   gradle run --args="produce <topic> <key|__NULL__> <count> [sleep-ms]"
 *   gradle run --args="consume <topic> <group-id>"
 *
 * 환경 변수로 동작을 바꿀 수 있습니다 (실험 3, 4 에서 필요):
 *   BOOTSTRAP      (default: localhost:9092,localhost:9094,localhost:9096)
 *   ACKS           (default: all)        — 0 / 1 / all
 *   IDEMPOTENCE    (default: true)       — true / false
 *   OFFSET_RESET   (default: earliest)   — earliest / latest / none
 *   CLIENT_ID      (default: auto)
 */
public class App {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            usage();
            System.exit(1);
        }
        String mode = args[0];
        String[] rest = new String[args.length - 1];
        System.arraycopy(args, 1, rest, 0, rest.length);

        switch (mode) {
            case "produce" -> SimpleProducer.main(rest);
            case "consume" -> SimpleConsumer.main(rest);
            default -> {
                System.err.println("Unknown mode: " + mode);
                usage();
                System.exit(1);
            }
        }
    }

    private static void usage() {
        System.err.println("Usage:");
        System.err.println("  produce <topic> <key|__NULL__> <count> [sleep-ms]");
        System.err.println("  consume <topic> <group-id>");
    }
}
