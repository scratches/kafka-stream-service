package com.example.demo;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.projectriff.grpc.function.FunctionProtos.Message;
import io.projectriff.grpc.function.ReactorMessageFunctionGrpc;

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
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.util.SocketUtils;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

@SpringBootApplication
@ConfigurationProperties("grpc")
public class StreamsApplication implements Closeable {

	@Autowired
	private ReceiverOptions<byte[], Message> receiverOptions;

	@Autowired
	private SenderOptions<byte[], Message> senderOptions;

	/**
	 * The port to connect to for gRPC connections.
	 */
	private int port = 10382;

	/**
	 * The host name to connect to the gRPC server.
	 */
	private String host = "localhost";

	/**
	 * Flag to enable or disable the gRPC server.
	 */
	private boolean enabled = true;

	private volatile boolean closed;

	public String getHost() {
		return this.host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		port = port > 0 ? port : SocketUtils.findAvailableTcpPort();
		this.port = port;
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	@Bean
	public ReceiverOptions<byte[], Message> receiver(KafkaProperties kafka) {
		kafka.getConsumer().setGroupId(UUID.randomUUID().toString());
		kafka.getConsumer().setKeyDeserializer(ByteArrayDeserializer.class);
		kafka.getConsumer().setValueDeserializer(MessageDeserializer.class);
		Map<String, Object> props = kafka.buildConsumerProperties();
		ReceiverOptions<byte[], Message> options = ReceiverOptions.create(props);
		options.consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		options.consumerProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
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
		return args -> {
			new Thread(() -> {
				try {
					StreamsApplication.this.run(args);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException(e);
				}
			}).start();
		};
	}

	private void run(String... args) throws InterruptedException {
		Disposable disposable = null;
		AtomicBoolean failed = new AtomicBoolean(false);
		while (!closed) {
			SenderOptions<byte[], Message> options = SenderOptions
					.create(senderOptions.producerProperties());
			if (failed.get()) {
				options.maxInFlight(1);
			}
			KafkaReceiver<byte[], Message> receiver = KafkaReceiver
					.create(receiverOptions);
			KafkaSender<byte[], Message> sender = KafkaSender.create(options);
			try {
				CountDownLatch latch = new CountDownLatch(1);
				disposable = receiver.receiveExactlyOnce(sender.transactionManager())
						.concatMap(records -> sender.send(extract(records.log()))
								.concatWith(
										sender.transactionManager().commit()
										))
						.onErrorResume(e -> {
							if (failed.compareAndSet(false, true)) {
								return sender.transactionManager().abort()
										.then(Mono.error(e));
							}
							else {
								// Already failed. There should be no outgoing messages.
								return sender.transactionManager().commit();
							}
						}).doOnComplete(() -> failed.set(false))
						.doOnTerminate(() -> 
						latch.countDown()
								).subscribe();
				latch.await();
			}
			finally {
				if (disposable != null) {
					disposable.dispose();
				}
			}
		}
	}

	private Flux<SenderRecord<byte[], Message, byte[]>> extract(
			Flux<ConsumerRecord<byte[], Message>> records) {
		return transform(records.map(ConsumerRecord::value)).map(this::output);
	}

	private Flux<Message> transform(Flux<Message> messages) {
		if (this.enabled) {
			ManagedChannel channel = ManagedChannelBuilder.forAddress(host, getPort())
					.usePlaintext(true).build();
			ReactorMessageFunctionGrpc.ReactorMessageFunctionStub stub = ReactorMessageFunctionGrpc
					.newReactorStub(channel);
			return Flux.create(emitter -> {
				stub.call(messages).subscribe(message -> emitter.next(message));
			});
		}
		return messages;
	}

	private SenderRecord<byte[], Message, byte[]> output(Message record) {
		// throw new RuntimeException("Planned");
		return SenderRecord.create(new ProducerRecord<>("replies", (byte[]) null, record),
				null);
	}

	public static void main(String[] args) {
		SpringApplication.run(StreamsApplication.class, args);
	}

	@Override
	public void close() throws IOException {
		this.closed = true;
	}

}
