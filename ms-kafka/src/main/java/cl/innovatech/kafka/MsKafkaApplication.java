package cl.innovatech.kafka;

import cl.innovatech.kafka.config.KafkaProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Microservicio de administracion y observabilidad del cluster de Kafka.
 *
 * Responsabilidades:
 *   - Declara la topologia de topicos al arrancar y expone por HTTP las
 *     operaciones de crear, describir, modificar y eliminar.
 *   - Publica el estado del cluster: particiones, offsets y retraso por grupo.
 *   - Mantiene un topico de pruebas con productor y consumidor propios.
 *
 * No participa del flujo de pedidos: si se detiene, el negocio sigue operando.
 */
@SpringBootApplication
@EnableConfigurationProperties(KafkaProperties.class)
public class MsKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(MsKafkaApplication.class, args);
    }
}
