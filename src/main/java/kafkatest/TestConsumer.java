package kafkatest;

import static kafkatest.KafkaProperties.TOPIC;
import static kafkatest.KafkaProperties.getConsumerProps;
import static kafkatest.Sleeper.sleepSec;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TestConsumer {

	public static void main(String[] args) {


		if (args.length <1) {
			println("parm need");
			println("java TestConsumer warmUpSec sleepSec maxPollIntervalSec sessionTimeOutSec loopCount host");
			return;
		}

		int warmUpSec = Integer.valueOf(args[0]);
		int sleepSec = Integer.valueOf(args[1]);
		int maxPollIntervalMs = Integer.valueOf(args[2]) * 1000;
		int sessionTimeOutMs = Integer.valueOf(args[3]) * 1000;
		int loopCount = Integer.valueOf(args[4]);
		String host = args[5];

		Properties props = getConsumerProps();
		props.put("max.poll.interval.ms", maxPollIntervalMs + "");
		props.put("session.timeout.ms", sessionTimeOutMs + "");
		props.put("bootstrap.servers", host + ":9092,localhost:9093,localhost:9094");

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

			consumer.subscribe(Arrays.asList(TOPIC));
			println("start subscribing");

			printf("warming up %s secs.. ", warmUpSec);
			sleepSec(warmUpSec);
			println("");

			for (int i = 0; i < loopCount; i++) {

				print("POLL()...");
				ConsumerRecords<String, String> records = consumer.poll(100);
				print(" fetched : " + records.count() + " records. ");

				printf("processing.. will take %s secs..", sleepSec);
				sleepSec(sleepSec);
				println(" done.");

				consumer.commitSync();
			}

			println("LOOP BROKE. Ctrl+C to stop main()");
			sleepSec(Integer.MAX_VALUE);
		}
	}

	private static void printf(String format, Object... args) {
		System.out.printf(format, args);
	}

	private static void print(String string) {
		System.out.print(string);
	}

	private static void println(String string) {
		System.out.println(string);
	}

}
