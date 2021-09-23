package com.example.demokafkaconsumernf;

import com.example.demokafkaconsumernf.model.Compra;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@SpringBootApplication
public class DemoKafkaConsumerNfApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoKafkaConsumerNfApplication.class, args);
        var consumer = new KafkaConsumer<String, String>(obterProperties());
        consumer.subscribe(Collections.singletonList("DKP.DKC.01"));

        while (true) {
            var msgs = consumer.poll(Duration.ofMillis(100));
            if (!msgs.isEmpty()) {
                msgsVerify(msgs);
            }
        }
    }

    private static void msgsVerify(ConsumerRecords<String, String> msgs) {
        for (var msg : msgs) {

            if(!msg.value().contains(",")){
                continue;
            }
        	var dado = msg.value().split(",");
        	var compra = getCompra(dado);
			System.out.println("----------------------------------------------------");
			System.out.println("Processando pedido "+compra.getId()+"... ");
        	validNF(compra);
        }
    }

	private static void validNF(Compra compra) {
    	if(compra.getValor().compareTo(BigDecimal.ZERO)==1){
    		sendNF();
		}
	}

	private static void sendNF() {
		System.out.println("NF enviada!");
	}

	private static Compra getCompra(String[] dado) {
		return Compra.builder()
				.id(Integer.valueOf(dado[0]))
				.produto(dado[1])
				.valor(new BigDecimal(dado[2]))
				.build();
	}


	private static Properties obterProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, DemoKafkaConsumerNfApplication.class.getSimpleName());
        return properties;
    }


}
