package kafkatest;

import static kafkatest.KafkaProperties.TOPIC;
import static kafkatest.KafkaProperties.getConsumerProps;
import static kafkatest.Sleeper.sleep;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TestConsumer {

	public static void main(String[] args) {

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProps())) {

			consumer.subscribe(Arrays.asList(TOPIC));

			while (true) {

				System.out.println("poll()...");
				ConsumerRecords<String, String> records = consumer.poll(100);
				System.out.println("polled 10 records : " + records.count());
				
//				consumer.pause(consumer.assignment());
//				System.out.println("paused()");

				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n",
										record.partition(), record.offset(), record.key(), record.value());
					sleep(500);
					if(record.offset() % 24 == 0) {
						System.out.println("long waiting...");
						sleep(10000);
					}
				}

				consumer.commitSync();
				System.out.println("commitSync()");

				sleep(1000);

//				consumer.resume(consumer.paused());
//				System.out.println("resume()");
			}
		}
	}

}
