package br.com.acbueno.kafka.producer;

import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import br.com.acbueno.kafka.config.Configuration;

public class ProducerRotation {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        var producer = new KafkaProducer<String, String>(Configuration.properties());
        var key = "Rotation";
        var value = "5000";
        var record = new ProducerRecord<String, String>("ROTATION_SENSOR", key, value);

        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Mensagem enviada com sucesso para: " + data.topic() + " | partition " + data.partition() + "| offset " + data.offset() + "| tempo " + data.timestamp());
        };
        producer.send(record, callback).get();
    }

}
