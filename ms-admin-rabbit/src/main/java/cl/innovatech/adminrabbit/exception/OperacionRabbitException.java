package cl.innovatech.adminrabbit.exception;

/** La operacion de administracion sobre RabbitMQ no pudo completarse. */
public class OperacionRabbitException extends RuntimeException {
    public OperacionRabbitException(String mensaje, Throwable causa) { super(mensaje, causa); }
}
