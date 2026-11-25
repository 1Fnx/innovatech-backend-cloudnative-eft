package cl.innovatech.adminrabbit.dto;

import jakarta.validation.constraints.*;

/** Datos validados para enlazar una cola con un exchange. */
public record CrearBindingRequest(

        @NotBlank(message = "El nombre de la cola es obligatorio")
        @Pattern(regexp = "^[a-zA-Z0-9._-]+$",
                 message = "El nombre de la cola solo admite letras, numeros, punto, guion y guion bajo")
        String cola,

        @NotBlank(message = "El nombre del exchange es obligatorio")
        @Pattern(regexp = "^[a-zA-Z0-9._-]+$",
                 message = "El nombre del exchange solo admite letras, numeros, punto, guion y guion bajo")
        String exchange,

        @NotNull(message = "La routing key es obligatoria (puede ir vacia para exchanges fanout)")
        @Size(max = 255, message = "La routing key no puede superar 255 caracteres")
        String routingKey
) { }
