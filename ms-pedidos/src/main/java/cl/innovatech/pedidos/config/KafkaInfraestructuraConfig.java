package cl.innovatech.pedidos.config;

import cl.innovatech.pedidos.messaging.evento.PedidoEvento;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Cliente productor de Kafka.
 *
 * Contraparte de RabbitInfraestructuraConfig para el log distribuido. Este
 * microservicio solo publica; la configuracion de consumidores vive en
 * ms-productos y ms-notificaciones.
 */
@Configuration
public class KafkaInfraestructuraConfig {

    /**
     * Lista de brokers para el primer contacto. El cliente obtiene de ahi el
     * mapa del cluster y luego habla con el lider de cada particion; se anotan
     * los tres para no depender de que uno concreto este disponible.
     */
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Fabrica de productores.
     *
     *   acks=all + idempotencia   la escritura se confirma cuando las replicas
     *                             sincronizadas la tienen, y un reintento del
     *                             cliente no deja el registro duplicado
     *   delivery.timeout.ms       ventana total de reintentos transitorios
     *   linger.ms=10              agrupa envios en lotes a cambio de una
     *                             latencia minima
     */
    @Bean
    public ProducerFactory<String, PedidoEvento> productorDeEventos() {
        Map<String, Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Clave en texto plano, valor en JSON.
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Durabilidad
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);

        // Rendimiento
        config.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // Sin el nombre de la clase Java en la cabecera: el contrato entre
        // microservicios es el JSON, no una clase concreta.
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, PedidoEvento> kafkaTemplate(
            ProducerFactory<String, PedidoEvento> productorDeEventos) {
        return new KafkaTemplate<>(productorDeEventos);
    }
}
