package cl.innovatech.notificaciones.exception;

/**
 * Fallo TRANSITORIO: tiene sentido volver a intentar el procesamiento.
 * Ejemplos: el servicio de correo no responde, la base de datos esta ocupada.
 *
 * El consumidor reencola el mensaje una vez; si vuelve a fallar, lo envia a la
 * Dead Letter Queue para evitar un ciclo infinito de reintentos.
 */
public class ErrorRecuperableException extends RuntimeException {
    public ErrorRecuperableException(String mensaje) { super(mensaje); }
    public ErrorRecuperableException(String mensaje, Throwable causa) { super(mensaje, causa); }
}
