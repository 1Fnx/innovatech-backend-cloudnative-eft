package cl.innovatech.kafka.dto;

import java.util.List;

/**
 * Brokers disponibles y broker controlador.
 *
 * @param coordinador id del broker que lleva el registro de particiones y
 *                    lideres; si cae, el ensemble elige otro
 */
public record EstadoCluster(
        String clusterId,
        int coordinador,
        List<BrokerInfo> brokers,
        int cantidadBrokers) {

    /** @param id identificador declarado en KAFKA_BROKER_ID */
    public record BrokerInfo(int id, String host, int puerto) {
    }
}
