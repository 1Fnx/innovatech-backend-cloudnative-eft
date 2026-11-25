package cl.innovatech.adminrabbit.dto;

import jakarta.validation.constraints.*;

/** Datos validados para crear un exchange. */
public record CrearExchangeRequest(

        @NotBlank(message = "El nombre del exchange es obligatorio")
        @Size(min = 1, max = 255, message = "El nombre debe tener entre 1 y 255 caracteres")
        @Pattern(regexp = "^[a-zA-Z0-9._-]+$",
                 message = "El nombre solo admite letras, numeros, punto, guion y guion bajo")
        String nombre,

        @NotBlank(message = "El tipo de exchange es obligatorio")
        @Pattern(regexp = "direct|topic|fanout|headers",
                 message = "El tipo debe ser direct, topic, fanout o headers")
        String tipo,

        Boolean durable,
        Boolean autoDelete
) {
    public boolean esDurable() { return durable == null || durable; }
    public boolean esAutoDelete() { return autoDelete != null && autoDelete; }
}
