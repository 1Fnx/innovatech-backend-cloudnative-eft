package cl.innovatech.adminrabbit.dto;

import jakarta.validation.constraints.*;

/**
 * Datos para crear una cola.
 *
 * El patron rechaza nombres vacios, con espacios o con caracteres que RabbitMQ
 * no admite. La validacion ocurre antes de llegar al servicio, de modo que una
 * peticion mal formada nunca alcanza al broker.
 */
public record CrearColaRequest(

        @NotBlank(message = "El nombre de la cola es obligatorio")
        @Size(min = 1, max = 255, message = "El nombre debe tener entre 1 y 255 caracteres")
        @Pattern(regexp = "^[a-zA-Z0-9._-]+$",
                 message = "El nombre solo admite letras, numeros, punto, guion y guion bajo")
        String nombre,

        /** Si es true la cola sobrevive al reinicio del broker. */
        Boolean durable,

        /** Si es true la cola se elimina sola cuando se desconecta su ultimo consumidor. */
        Boolean autoDelete,

        /** Exchange al que se enviaran los mensajes descartados por esta cola. */
        @Size(max = 255, message = "El nombre del DLX no puede superar 255 caracteres")
        @Pattern(regexp = "^$|^[a-zA-Z0-9._-]+$",
                 message = "El DLX solo admite letras, numeros, punto, guion y guion bajo")
        String deadLetterExchange,

        @Size(max = 255, message = "La routing key del DLX no puede superar 255 caracteres")
        String deadLetterRoutingKey,

        /** Tiempo de vida de los mensajes en milisegundos. Opcional. */
        @Positive(message = "El TTL debe ser un numero positivo de milisegundos")
        Integer mensajeTtlMs,

        /** Maximo numero de mensajes que admite la cola. Opcional. */
        @Positive(message = "La longitud maxima debe ser un numero positivo")
        Integer longitudMaxima
) {
    public boolean esDurable() { return durable == null || durable; }
    public boolean esAutoDelete() { return autoDelete != null && autoDelete; }
}
