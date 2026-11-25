package cl.innovatech.adminrabbit.controller;

import cl.innovatech.adminrabbit.exception.OperacionRabbitException;
import cl.innovatech.adminrabbit.exception.RecursoNoEncontradoException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Traduce las excepciones de la API administradora a respuestas HTTP claras.
 *
 * Cuando la validacion falla, el cliente recibe un 400 con el detalle campo
 * por campo en lugar de un 500 generico, lo que hace el error accionable desde
 * el lado del consumidor de la API.
 */
@RestControllerAdvice
public class ManejadorDeErrores {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> datosInvalidos(MethodArgumentNotValidException ex) {
        Map<String, String> campos = new LinkedHashMap<>();
        for (FieldError error : ex.getBindingResult().getFieldErrors()) {
            campos.put(error.getField(), error.getDefaultMessage());
        }
        Map<String, Object> cuerpo = new LinkedHashMap<>();
        cuerpo.put("error", "Parametros de entrada invalidos");
        cuerpo.put("detalles", campos);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(cuerpo);
    }

    @ExceptionHandler(RecursoNoEncontradoException.class)
    public ResponseEntity<Map<String, Object>> noEncontrado(RecursoNoEncontradoException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "Recurso no encontrado", "detalle", ex.getMessage()));
    }

    @ExceptionHandler(OperacionRabbitException.class)
    public ResponseEntity<Map<String, Object>> operacionFallida(OperacionRabbitException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("error", "La operacion sobre RabbitMQ no pudo completarse",
                        "detalle", ex.getMessage()));
    }
}
