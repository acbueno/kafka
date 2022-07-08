package br.com.acbueno.kafka.consumer;

import java.time.Duration;
import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import br.com.acbueno.kafka.config.Configuration;

public class SensorRotationConsumer {

    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(Configuration.getProperties());
        consumer.subscribe(Collections.singletonList("ROTATION_SENSOR"));

        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> registro : records) {
                System.out.println("------------------------------------------");
                System.out.println("RPM: ");
                System.out.println(registro.key());
                System.out.println(registro.value());

                final String valor = registro.value();
                final Integer rotation = Integer.valueOf(valor);
                if (rotation > 6000) {
                    System.out.println("High Rotation");
                } else if (rotation < 1000) {
                    System.out.println("Low Rotation");
                } else if(rotation <=5000) {
                    System.out.println("Work Rotation");
                }

                System.out.println("Data received");
            }
        }
    }

}
