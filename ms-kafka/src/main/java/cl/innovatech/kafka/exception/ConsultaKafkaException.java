package cl.innovatech.kafka.exception;

/**
 * Se lanza cuando no se puede obtener informacion del cluster: el broker no
 * responde, el topic no existe o la consulta supera el tiempo limite.
 *
 * Tener una excepcion propia permite que el manejador de errores devuelva un
 * codigo HTTP adecuado en lugar de un 500 generico, y que el frontend muestre
 * un aviso entendible en vez de una traza de Java.
 */
public class ConsultaKafkaException extends RuntimeException {

    public ConsultaKafkaException(String mensaje, Throwable causa) {
        super(mensaje, causa);
    }
}
