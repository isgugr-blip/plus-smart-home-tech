package ru.yandex.practicum.collector.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.collector.dto.HubEvent;
import ru.yandex.practicum.collector.dto.SensorEvent;
import ru.yandex.practicum.collector.service.HubService;
import ru.yandex.practicum.collector.service.SensorService;

@RestController
@RequestMapping("/events")
@RequiredArgsConstructor
public class EventController {

    private final SensorService sensorService;
    private final HubService hubService;

    @PostMapping("/sensors")
    public void collectSensorEvent(@Valid @RequestBody SensorEvent event) {
        sensorService.send(event);
    }

    @PostMapping("/hubs")
    public void collectHubEvent(@Valid @RequestBody HubEvent event) {
        hubService.send(event);
    }
}
