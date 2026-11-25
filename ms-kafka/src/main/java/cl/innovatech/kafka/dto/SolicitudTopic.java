package cl.innovatech.kafka.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * Datos de creacion de un topico.
 *
 * Particiones, replicas y retencion son opcionales: ausentes, se aplican los
 * valores por omision del proyecto.
 *
 * El patron del nombre replica el que acepta Kafka. Validarlo aqui evita que
 * el nombre llegue al broker y vuelva como un error poco descriptivo.
 */
public record SolicitudTopic(

        @NotBlank(message = "El nombre del topic es obligatorio")
        @Size(max = 200, message = "El nombre no puede superar los 200 caracteres")
        @Pattern(regexp = "^[A-Za-z0-9._\\-]+$",
                 message = "El nombre solo admite letras, numeros, punto, guion y guion bajo")
        String nombre,

        @Min(value = 1, message = "Debe haber al menos una particion")
        @Max(value = 50, message = "El maximo admitido es 50 particiones")
        Integer particiones,

        @Min(value = 1, message = "Debe haber al menos una replica")
        @Max(value = 3, message = "El cluster tiene tres brokers, por lo que el maximo es 3")
        Integer replicas,

        @Min(value = 60000, message = "La retencion minima admitida es de un minuto")
        Long retencionMs,

        boolean crearDlt) {
}
