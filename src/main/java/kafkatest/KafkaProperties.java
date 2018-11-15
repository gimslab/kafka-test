package kafkatest;

import java.util.Properties;

public class KafkaProperties {

	public static final String TOPIC = "test";

	public static Properties getProducerProps() {
		Properties props = getBasicProps();
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

	public static Properties getConsumerProps() {
		Properties props = getBasicProps();
		props.put("group.id", "consumer-group-1");
		props.put("enable.auto.commit", "false");
//		props.put("auto.commit.interval.ms", "1000");
		props.put("max.poll.records", "10");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}

	private static Properties getBasicProps() {
		Properties props = new Properties();
//		props.put("bootstrap.servers", "localhost:9092");
		props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		return props;
	}
}
