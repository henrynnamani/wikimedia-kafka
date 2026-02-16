package org.example;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaProducer {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServer = "localhost:9092";

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", bootstrapServer);

        properties.setProperty("key.serializer",
                StringSerializer.class.getName());
        properties.setProperty("value.serializer",
                StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url)).headers(Headers.of(Map.of(
                "User-Agent", "my-kafka-stream-app/1.0 (henrynnamani12304@gmail.com)"
        )));;
        EventSource eventSource = builder.build();

        eventSource.start();

        TimeUnit.MINUTES.sleep(10);
    }
}
