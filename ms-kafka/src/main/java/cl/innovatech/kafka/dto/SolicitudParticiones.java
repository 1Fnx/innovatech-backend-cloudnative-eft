package cl.innovatech.kafka.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

/**
 * Cantidad total de particiones deseada. Debe ser mayor que la actual: Kafka
 * no admite reducir, porque el destino de una clave es
 * hash(clave) % particiones y las claves ya publicadas cambiarian de particion.
 */
public record SolicitudParticiones(

        @Min(value = 1, message = "La cantidad de particiones debe ser al menos 1")
        @Max(value = 50, message = "El maximo admitido es 50 particiones")
        int particiones) {
}
