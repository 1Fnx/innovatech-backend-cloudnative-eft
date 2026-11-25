package cl.innovatech.productos.config;

import cl.innovatech.productos.exception.ErrorNoRecuperableException;
import cl.innovatech.productos.messaging.evento.MovimientoInventarioEvento;
import cl.innovatech.productos.messaging.evento.PedidoEvento;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Cliente de Kafka de ms-productos.
 *
 * El microservicio consume del topico de pedidos y publica en el de
 * movimientos, de modo que aqui conviven la configuracion de consumidor y la
 * de productor.
 */
@Configuration
public class KafkaInfraestructuraConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaInfraestructuraConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.grupos.inventario}")
    private String grupoInventario;

    // =========================================================================
    //  Consumo
    // =========================================================================

    /**
     * Fabrica de consumidores.
     *
     *   auto-offset-reset=earliest   solo actua si el grupo no tiene offset
     *                                confirmado valido
     *   enable-auto-commit=false     el offset avanza cuando el listener
     *                                termina sin lanzar, no por tiempo
     *   max.poll.records=10          lotes acotados para que un reintento no
     *                                arrastre demasiados registros
     *
     * ErrorHandlingDeserializer envuelve al deserializador JSON: sin el, un
     * payload mal formado lanza antes de que exista el listener y el contenedor
     * reintenta el mismo offset indefinidamente.
     */
    @Bean
    public ConsumerFactory<String, PedidoEvento> consumidorDePedidos() {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, grupoInventario);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, PedidoEvento.class.getName());
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "cl.innovatech.*");
        config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(config);
    }

    /**
     * Contenedor de listeners y politica de errores.
     *
     * Dos reintentos separados por dos segundos y, agotados, el registro se
     * publica en el topico .DLT. ErrorNoRecuperableException se marca como no
     * reintentable: un payload invalido falla igual las tres veces y mientras
     * se reintenta bloquea la particion.
     *
     * A diferencia de AMQP, aqui el mensaje no vuelve a la cola: lo que avanza
     * o no avanza es el offset del grupo.
     *
     * concurrency=3, una instancia por particion; un grupo no puede tener mas
     * consumidores activos que particiones.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PedidoEvento> kafkaListenerContainerFactory(
            ConsumerFactory<String, PedidoEvento> consumidorDePedidos,
            KafkaTemplate<String, Object> plantillaParaDlt) {

        DeadLetterPublishingRecoverer aDeadLetterTopic =
                new DeadLetterPublishingRecoverer(plantillaParaDlt);

        DefaultErrorHandler manejador = new DefaultErrorHandler(
                aDeadLetterTopic, new FixedBackOff(2000L, 2L));

        manejador.addNotRetryableExceptions(ErrorNoRecuperableException.class);

        manejador.setRetryListeners((registro, excepcion, intento) ->
                log.warn("[INVENTARIO KAFKA] Reintento {} para offset={} particion={} causa={}",
                        intento, registro.offset(), registro.partition(), excepcion.getMessage()));

        ConcurrentKafkaListenerContainerFactory<String, PedidoEvento> fabrica =
                new ConcurrentKafkaListenerContainerFactory<>();
        fabrica.setConsumerFactory(consumidorDePedidos);
        fabrica.setCommonErrorHandler(manejador);
        fabrica.setConcurrency(3);
        return fabrica;
    }

    // =========================================================================
    //  Publicacion
    // =========================================================================

    @Bean
    public ProducerFactory<String, MovimientoInventarioEvento> productorDeMovimientos() {
        return new DefaultKafkaProducerFactory<>(configuracionDeProductor());
    }

    @Bean
    public KafkaTemplate<String, MovimientoInventarioEvento> kafkaTemplateMovimientos(
            ProducerFactory<String, MovimientoInventarioEvento> productorDeMovimientos) {
        return new KafkaTemplate<>(productorDeMovimientos);
    }

    /**
     * Plantilla del recuperador de descartes. El valor es Object porque se
     * reenvia el registro original tal como llego, sea cual sea su tipo.
     */
    @Bean
    public KafkaTemplate<String, Object> plantillaParaDlt() {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configuracionDeProductor()));
    }

    /** Configuracion comun de los dos productores. */
    private Map<String, Object> configuracionDeProductor() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return config;
    }
}
