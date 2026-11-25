package cl.innovatech.notificaciones.config;

import cl.innovatech.notificaciones.exception.ErrorNoRecuperableException;
import cl.innovatech.notificaciones.messaging.evento.MovimientoInventarioEvento;
import cl.innovatech.notificaciones.messaging.evento.PedidoEvento;
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
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Cliente de Kafka de ms-notificaciones.
 *
 * El microservicio consume los dos topicos del sistema, y cada uno transporta
 * un tipo distinto de mensaje: de ahi dos fabricas de consumidores y dos
 * contenedores. Un mismo deserializador no puede devolver a la vez un
 * PedidoEvento y un MovimientoInventarioEvento.
 *
 * Cada listener elige su contenedor con el atributo containerFactory.
 */
@Configuration
public class KafkaInfraestructuraConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaInfraestructuraConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.grupos.notificaciones}")
    private String grupoNotificaciones;

    @Value("${app.kafka.grupos.alertas}")
    private String grupoAlertas;

    // =========================================================================
    //  Topico de pedidos -> grupo de notificaciones
    // =========================================================================

    @Bean
    public ConsumerFactory<String, PedidoEvento> consumidorDePedidos() {
        return new DefaultKafkaConsumerFactory<>(
                configuracionDeConsumidor(grupoNotificaciones, PedidoEvento.class));
    }

    /**
     * Se registra con dos nombres: el que usa el listener y el que busca la
     * autoconfiguracion de Spring Boot. Al existir ya un bean llamado
     * kafkaListenerContainerFactory, la autoconfiguracion no crea el suyo.
     */
    @Bean(name = {"kafkaListenerContainerFactory", "fabricaPedidos"})
    public ConcurrentKafkaListenerContainerFactory<String, PedidoEvento> fabricaPedidos(
            ConsumerFactory<String, PedidoEvento> consumidorDePedidos,
            KafkaTemplate<String, Object> plantillaParaDlt) {

        ConcurrentKafkaListenerContainerFactory<String, PedidoEvento> fabrica =
                new ConcurrentKafkaListenerContainerFactory<>();
        fabrica.setConsumerFactory(consumidorDePedidos);
        fabrica.setCommonErrorHandler(manejadorDeErrores(plantillaParaDlt, "NOTIFICACIONES"));
        fabrica.setConcurrency(3);
        return fabrica;
    }

    // =========================================================================
    //  Topico de movimientos -> grupo de alertas
    // =========================================================================

    @Bean
    public ConsumerFactory<String, MovimientoInventarioEvento> consumidorDeMovimientos() {
        return new DefaultKafkaConsumerFactory<>(
                configuracionDeConsumidor(grupoAlertas, MovimientoInventarioEvento.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MovimientoInventarioEvento> fabricaMovimientos(
            ConsumerFactory<String, MovimientoInventarioEvento> consumidorDeMovimientos,
            KafkaTemplate<String, Object> plantillaParaDlt) {

        ConcurrentKafkaListenerContainerFactory<String, MovimientoInventarioEvento> fabrica =
                new ConcurrentKafkaListenerContainerFactory<>();
        fabrica.setConsumerFactory(consumidorDeMovimientos);
        fabrica.setCommonErrorHandler(manejadorDeErrores(plantillaParaDlt, "ALERTAS"));
        fabrica.setConcurrency(3);
        return fabrica;
    }

    // =========================================================================
    //  Piezas compartidas
    // =========================================================================

    /**
     * Configuracion comun de los consumidores; solo cambian el grupo y el tipo
     * esperado.
     *
     * ErrorHandlingDeserializer envuelve al deserializador JSON: sin el, un
     * payload mal formado lanza antes de que exista el listener y el contenedor
     * reintenta el mismo offset indefinidamente.
     */
    private Map<String, Object> configuracionDeConsumidor(String grupo, Class<?> tipoEsperado) {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, grupo);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, tipoEsperado.getName());
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "cl.innovatech.*");
        config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return config;
    }

    /**
     * Dos reintentos separados por dos segundos y, agotados, el registro se
     * publica en el topico .DLT.
     *
     * ErrorNoRecuperableException se marca como no reintentable: un payload
     * invalido falla igual las tres veces y mientras se reintenta bloquea la
     * particion para los mensajes que vienen detras.
     */
    private DefaultErrorHandler manejadorDeErrores(KafkaTemplate<String, Object> plantilla,
                                                   String dominio) {
        DefaultErrorHandler manejador = new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(plantilla),
                new FixedBackOff(2000L, 2L));

        manejador.addNotRetryableExceptions(ErrorNoRecuperableException.class);

        manejador.setRetryListeners((registro, excepcion, intento) ->
                log.warn("[{} KAFKA] Reintento {} para topic={} particion={} offset={} causa={}",
                        dominio, intento, registro.topic(), registro.partition(),
                        registro.offset(), excepcion.getMessage()));

        return manejador;
    }

    /**
     * Plantilla del recuperador de descartes. El valor es Object porque se
     * reenvia el registro original tal como llego.
     */
    @Bean
    public KafkaTemplate<String, Object> plantillaParaDlt() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
    }
}
