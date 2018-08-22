package kafkatest;

import static kafkatest.KafkaProperties.TOPIC;
import static kafkatest.KafkaProperties.getProducerProps;
import static kafkatest.Sleeper.sleep;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestProducer {

	public static void main(String[] args) {

		Producer<String, String> producer = new KafkaProducer<>(getProducerProps());

		for (int i = 0; i < Integer.MAX_VALUE; i++) {
			producer.send(new ProducerRecord<String, String>(TOPIC, i + "", i + ""));
			System.out.println("sent to " + TOPIC + " : " + i);
			sleep(50);
		}

		producer.close();
	}
}
