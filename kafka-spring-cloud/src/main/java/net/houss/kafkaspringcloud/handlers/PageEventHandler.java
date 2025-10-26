package net.houss.kafkaspringcloud.handlers;

import net.houss.kafkaspringcloud.events.PageEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Component
public class PageEventHandler {

    // doesn't have an output
    @Bean
    public Consumer<PageEvent> pageEventConsumer(){
        return (input) -> {
            System.out.println("***************");
            System.out.println(input.toString());
            System.out.println("***************");
        };
    }

    // doesn't have an input
    @Bean
    public Supplier<PageEvent> pageEventSupplier(){
        return () -> {
            return new PageEvent(
                    Math.random() > 0.5 ? "P1" : "P2",
                    Math.random() > 0.5 ? "U1" : "U2",
                    new Date(),
                    10 + new Random().nextInt(10000)
            );
        };
    }

    // does have an input and an output
    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Long>> kStreamKStreamFunction(){
        return input -> {
            return input.filter((k, v) -> v.duration() > 100)
                    .map((k, v) -> new KeyValue<>(v.name(), v.duration()));
        };
    }
}
