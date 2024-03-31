package com.akash.Kafkaproducerwikimedia.producer;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class WikimediaChangeProducer {

    @Value("${kafka.topic.name}")
    private String topicName;
    private final Logger logger= LoggerFactory.getLogger(WikimediaChangeProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage() throws InterruptedException {
        String topic=topicName;

        //To read real time data we have to use event source

        BackgroundEventHandler backgroundEventHandler= new WikimediaChangesHandler(kafkaTemplate, topic);

        String url= "https://stream.wikimedia.org/v2/stream/recentchange";

        BackgroundEventSource.Builder builder= new BackgroundEventSource.Builder(backgroundEventHandler,
                new EventSource.Builder(
                        ConnectStrategy.http(URI.create(url))
                                .connectTimeout(5, TimeUnit.SECONDS)
                ));
        BackgroundEventSource backgroundEventSource= builder.build();
        backgroundEventSource.start();
        TimeUnit.MINUTES.sleep(10);
    }

}
