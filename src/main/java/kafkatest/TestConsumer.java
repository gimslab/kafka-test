package kafkatest;

import static kafkatest.KafkaProperties.TOPIC;
import static kafkatest.KafkaProperties.getConsumerProps;
import static kafkatest.Sleeper.sleepSec;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class TestConsumer {

	public static void main(String[] args) {


		if (args.length <1) {
			println("parm need");
			println("java TestConsumer sleepSec maxPollIntervalSec sessionTimeOutSec loopCount host");
			return;
		}

		int sleepSec = Integer.valueOf(args[0]);
		int maxPollIntervalMs = Integer.valueOf(args[1]) * 1000;
		int sessionTimeOutMs = Integer.valueOf(args[2]) * 1000;
		int loopCount = Integer.valueOf(args[3]);
		String host = args[4];

		Properties props = getConsumerProps();
		props.put("max.poll.interval.ms", maxPollIntervalMs + "");
		props.put("session.timeout.ms", sessionTimeOutMs + "");
		props.put("bootstrap.servers", host + ":9092,localhost:9093,localhost:9094");

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

			consumer.subscribe(Arrays.asList(TOPIC));

			for (int i = 0; i < loopCount; i++) {
				if (i == 2) {
					consumer.pause(consumer.assignment());
					print("Paused. ");
				}
				if (i == 5) {
					consumer.resume(consumer.assignment());
					print("Resumed. ");
				}
				pollAndProcess(consumer, sleepSec);
			}

			println("LOOP BROKE. waiting.. Ctrl+C to stop main()");
			sleepSec(Integer.MAX_VALUE);
		}
	}

	private static void pollAndProcess(KafkaConsumer<String, String> consumer, int sleepSec) {
		print("POLL()...");
		ConsumerRecords<String, String> records = consumer.poll(100);
		print(" fetched : " + records.count() + " records. ");

		printf("processing.. will take %s secs..", sleepSec);
		sleepSec(sleepSec);
		println(" done.");

		try {
			consumer.commitSync();
		} catch (CommitFailedException cfe) {
			cfe.printStackTrace();
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
