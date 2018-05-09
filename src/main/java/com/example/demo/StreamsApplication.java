package com.example.demo;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import io.projectriff.grpc.function.FunctionProtos.Message;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

@SpringBootApplication
public class StreamsApplication implements Closeable {

	private CountDownLatch latch = new CountDownLatch(1);

	@Autowired
	private ReceiverOptions<byte[], Message> receiverOptions;

	@Autowired
	private SenderOptions<byte[], Message> senderOptions;

	@Override
	public void close() {
		latch.countDown();
	}

	@Bean
	public ReceiverOptions<byte[], Message> receiver(KafkaProperties kafka) {
		kafka.getConsumer().setGroupId(UUID.randomUUID().toString());
		kafka.getConsumer().setKeyDeserializer(ByteArrayDeserializer.class);
		kafka.getConsumer().setValueDeserializer(MessageDeserializer.class);
		Map<String, Object> props = kafka.buildConsumerProperties();
		ReceiverOptions<byte[], Message> options = ReceiverOptions.create(props);
		options.consumerProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		options.subscription(Arrays.asList("uppercase"));
		return options;
	}

	@Bean
	public SenderOptions<byte[], Message> sender(KafkaProperties kafka) {
		kafka.getProducer().setKeySerializer(ByteArraySerializer.class);
		kafka.getProducer().setValueSerializer(MessageSerializer.class);
		Map<String, Object> props = kafka.buildProducerProperties();
		props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
		SenderOptions<byte[], Message> options = SenderOptions.create(props);
		return options;
	}

	@Bean
	public CommandLineRunner runner() {
		return this::run;
	}

	private void run(String... args) throws InterruptedException {
		Disposable disposable = null;
		KafkaReceiver<byte[], Message> receiver = KafkaReceiver.create(receiverOptions);
		KafkaSender<byte[], Message> sender = KafkaSender.create(senderOptions);
		try {
			disposable = receiver.receiveExactlyOnce(sender.transactionManager())
					.concatMap(records -> sender.send(extract(records))
							.concatWith(sender.transactionManager().commit()))
					.onErrorResume(
							e -> sender.transactionManager().abort().then(Mono.error(e)))
					.subscribe();
			latch.await();
		}
		finally {
			if (disposable != null) {
				disposable.dispose();
			}
		}

	}

	private Flux<SenderRecord<byte[], Message, byte[]>> extract(
			Flux<ConsumerRecord<byte[], Message>> records) {
		return transform(records.map(ConsumerRecord::value)).map(this::output).log();
	}

	private Flux<Message> transform(Flux<Message> messages) {
		return messages;
	}

	private SenderRecord<byte[], Message, byte[]> output(
			Message record) {
//		throw new RuntimeException("Planned");
		return SenderRecord.create(
				new ProducerRecord<>("words", (byte[]) null, record), null);
	}

	public static void main(String[] args) {
		SpringApplication.run(StreamsApplication.class, args);
	}

}
