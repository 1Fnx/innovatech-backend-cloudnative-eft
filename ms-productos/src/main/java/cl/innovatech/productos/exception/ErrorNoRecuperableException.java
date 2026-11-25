package cl.innovatech.productos.exception;

/**
 * Fallo PERMANENTE: reintentar no sirve de nada porque el mensaje nunca se
 * podra procesar. Ejemplos: falta un campo obligatorio, el formato es invalido.
 *
 * El consumidor lo rechaza de inmediato sin reencolar, de modo que viaja
 * directamente a la Dead Letter Queue para su analisis posterior.
 */
public class ErrorNoRecuperableException extends RuntimeException {
    public ErrorNoRecuperableException(String mensaje) { super(mensaje); }
    public ErrorNoRecuperableException(String mensaje, Throwable causa) { super(mensaje, causa); }
}
