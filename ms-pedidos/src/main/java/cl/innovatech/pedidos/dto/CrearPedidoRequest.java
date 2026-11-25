package cl.innovatech.pedidos.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.List;

/**
 * Datos de entrada para crear un pedido.
 * Las anotaciones de validacion impiden que llegue basura a la logica de negocio
 * y, por lo tanto, que se publiquen eventos invalidos en RabbitMQ.
 */
public record CrearPedidoRequest(

        @NotBlank(message = "El nombre del cliente es obligatorio")
        @Size(max = 120, message = "El nombre del cliente no puede superar 120 caracteres")
        String cliente,

        @NotBlank(message = "El correo es obligatorio")
        @Email(message = "El correo no tiene un formato valido")
        String email,

        @NotEmpty(message = "El pedido debe incluir al menos un producto")
        @Valid
        List<ItemRequest> items,

        /**
         * Conmutador de pruebas: fuerza un error en los consumidores para
         * ejercitar los reintentos y el envio a la DLQ. En uso normal viaja
         * como NINGUNA.
         */
        @Pattern(regexp = "NINGUNA|ERROR_RECUPERABLE|ERROR_NO_RECUPERABLE",
                 message = "simulacion debe ser NINGUNA, ERROR_RECUPERABLE o ERROR_NO_RECUPERABLE")
        String simulacion
) {
    public record ItemRequest(
            @NotNull(message = "El id del producto es obligatorio")
            Long productoId,

            @NotBlank(message = "El nombre del producto es obligatorio")
            String nombre,

            @NotNull(message = "La cantidad es obligatoria")
            @Min(value = 1, message = "La cantidad debe ser al menos 1")
            Integer cantidad,

            @NotNull(message = "El precio es obligatorio")
            @Positive(message = "El precio debe ser mayor que cero")
            Double precio
    ) { }
}
