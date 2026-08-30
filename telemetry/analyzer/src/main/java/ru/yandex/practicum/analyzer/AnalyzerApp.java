package ru.yandex.practicum.analyzer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.analyzer.processor.HubEventProcessor;
import ru.yandex.practicum.analyzer.processor.SnapshotProcessor;

@SpringBootApplication
public class AnalyzerApp {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(AnalyzerApp.class, args);

        Thread hubEventsThread = new Thread(context.getBean(HubEventProcessor.class));
        hubEventsThread.setName("HubEventHandlerThread");
        hubEventsThread.start();

        context.getBean(SnapshotProcessor.class).start();
    }
}
