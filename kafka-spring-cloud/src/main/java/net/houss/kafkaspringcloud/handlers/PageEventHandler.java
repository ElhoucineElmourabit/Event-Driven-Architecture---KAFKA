package net.houss.kafkaspringcloud.handlers;

import net.houss.kafkaspringcloud.events.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
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
                    .map((k, v) -> new KeyValue<>(v.name(), v.duration()))
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                    .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
                    .count(Materialized.as("count-store"))
                    .toStream()
                    .map((k,v) -> new KeyValue<>(k.key(), v))
                    ;
        };
    }
}
