package cl.innovatech.adminrabbit.exception;

/** El recurso solicitado (cola o exchange) no existe en el broker. */
public class RecursoNoEncontradoException extends RuntimeException {
    public RecursoNoEncontradoException(String mensaje) { super(mensaje); }
}
