package cl.innovatech.kafka.exception;

/**
 * Operacion de administracion invalida o no permitida: reducir particiones,
 * eliminar un topico interno del cluster, borrar un topico del negocio sin
 * confirmar, o actuar sobre un topico inexistente.
 *
 * Se traduce a 400. ConsultaKafkaException, en cambio, significa que el cluster
 * no responde y se traduce a 503.
 */
public class OperacionKafkaException extends RuntimeException {

    public OperacionKafkaException(String mensaje) {
        super(mensaje);
    }

    public OperacionKafkaException(String mensaje, Throwable causa) {
        super(mensaje, causa);
    }
}
