package ru.yandex.practicum.collector.service;

import ru.yandex.practicum.collector.dto.SensorEvent;

public interface SensorService {
    void send(SensorEvent event);
}
