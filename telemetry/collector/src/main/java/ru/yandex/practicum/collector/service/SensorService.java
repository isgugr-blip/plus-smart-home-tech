package ru.yandex.practicum.collector.service;

import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

public interface SensorService {
    void send(SensorEventProto event);
}
