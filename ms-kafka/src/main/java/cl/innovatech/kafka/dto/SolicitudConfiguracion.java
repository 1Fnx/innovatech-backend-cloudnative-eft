package cl.innovatech.kafka.dto;

import jakarta.validation.constraints.NotEmpty;

import java.util.Map;

/**
 * Parametros de configuracion a aplicar sobre un topico existente, con las
 * claves que define Kafka: retention.ms, cleanup.policy, min.insync.replicas,
 * max.message.bytes y similares.
 */
public record SolicitudConfiguracion(

        @NotEmpty(message = "Debe indicar al menos un parametro de configuracion")
        Map<String, String> configuracion) {
}
