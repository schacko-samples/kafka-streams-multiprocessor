package kafka.streams.multiprocessor.kafkastreamsmultiprocessor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.messaging.handler.annotation.SendTo;

import java.util.Arrays;
import java.util.Date;

@SpringBootApplication
@EnableBinding(KafkaStreamsProcessorX.class)
public class KafkaStreamsMultiprocessorApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsMultiprocessorApplication.class, args);
	}

	@StreamListener("input")
	@SendTo("output")
	public KStream<?, WordCount> process(KStream<Object, String> input) {

		return input
				.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
				.map((key, value) -> new KeyValue<>(value, value))
				.groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
				.windowedBy(TimeWindows.of(300000))
				.count(Materialized.as("WordCounts-2"))
				.toStream()
				.map((key, value) -> new KeyValue<>(null, new WordCount(key.key(), value, new Date(key.window().start()), new Date(key.window().end()))));
	}

	@StreamListener("foobar")
	public void processX(KStream<Object, String> input) {

		input.foreach((k, v) -> System.out.println("printing..." + v));
	}

	static class WordCount {

		private String word;

		private long count;

		private Date start;

		private Date end;

		WordCount(String word, long count, Date start, Date end) {
			this.word = word;
			this.count = count;
			this.start = start;
			this.end = end;
		}

		public String getWord() {
			return word;
		}

		public void setWord(String word) {
			this.word = word;
		}

		public long getCount() {
			return count;
		}

		public void setCount(long count) {
			this.count = count;
		}

		public Date getStart() {
			return start;
		}

		public void setStart(Date start) {
			this.start = start;
		}

		public Date getEnd() {
			return end;
		}

		public void setEnd(Date end) {
			this.end = end;
		}
	}
}

interface KafkaStreamsProcessorX extends KafkaStreamsProcessor {

	@Input("foobar")
	KStream<?, ?> inputX();

}
