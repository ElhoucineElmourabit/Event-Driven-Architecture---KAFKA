package net.houss.kafkaspringcloud.handlers;

import net.houss.kafkaspringcloud.events.PageEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Component
public class PageEventHandler {

    @Bean
    public Consumer<PageEvent> pageEventConsumer(){
        return (input) -> {
            System.out.println("***************");
            System.out.println(input.toString());
            System.out.println("***************");
        };
    }
}
