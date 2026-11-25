package cl.innovatech.kafka.controller;

import cl.innovatech.kafka.exception.ConsultaKafkaException;
import cl.innovatech.kafka.exception.OperacionKafkaException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.ErrorResponseException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.validation.FieldError;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Traduccion de excepciones a respuestas HTTP.
 *
 * El cuerpo de error mantiene una forma estable (estado, error, mensaje y, si
 * corresponde, detalles por campo) para que el cliente pueda mostrarlo sin
 * interpretar trazas.
 */
@RestControllerAdvice
public class ManejadorDeErrores {

    private static final Logger log = LoggerFactory.getLogger(ManejadorDeErrores.class);

    /**
     * Parametros que no cumplen las restricciones declaradas en el controlador.
     */
    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<Map<String, Object>> parametrosInvalidos(ConstraintViolationException e) {
        Map<String, String> detalles = new LinkedHashMap<>();
        for (ConstraintViolation<?> violacion : e.getConstraintViolations()) {
            String ruta = violacion.getPropertyPath().toString();
            // La ruta llega como "metodo.parametro"; se conserva solo el parametro.
            String campo = ruta.contains(".") ? ruta.substring(ruta.lastIndexOf('.') + 1) : ruta;
            detalles.put(campo, violacion.getMessage());
        }
        return respuesta(HttpStatus.BAD_REQUEST, "Parametros invalidos", detalles);
    }

    /** Cuerpo JSON que no cumple las validaciones del DTO. No llega al cluster. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> cuerpoInvalido(MethodArgumentNotValidException e) {
        Map<String, String> detalles = new LinkedHashMap<>();
        for (FieldError error : e.getBindingResult().getFieldErrors()) {
            detalles.put(error.getField(), error.getDefaultMessage());
        }
        return respuesta(HttpStatus.BAD_REQUEST, "Datos invalidos en el cuerpo de la peticion", detalles);
    }

    /**
     * Operacion rechazada por la propia aplicacion.
     *
     * 400 y no 503: el cluster esta disponible, lo invalido es la peticion.
     */
    @ExceptionHandler(OperacionKafkaException.class)
    public ResponseEntity<Map<String, Object>> operacionInvalida(OperacionKafkaException e) {
        log.warn("Operacion de administracion rechazada: {}", e.getMessage());
        return respuesta(HttpStatus.BAD_REQUEST, e.getMessage(), null);
    }

    /** Falta un parametro obligatorio, por ejemplo publicar sin "message". */
    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<Map<String, Object>> parametroAusente(MissingServletRequestParameterException e) {
        return respuesta(HttpStatus.BAD_REQUEST,
                "Falta el parametro obligatorio: " + e.getParameterName(), null);
    }

    /** Verbo no admitido en una ruta que si existe. */
    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<Map<String, Object>> metodoNoAdmitido(HttpRequestMethodNotSupportedException e) {
        return respuesta(HttpStatus.METHOD_NOT_ALLOWED,
                "El metodo " + e.getMethod() + " no esta permitido en esta ruta. "
                        + "Revise el verbo: POST para crear, PATCH para modificar, DELETE para eliminar.",
                null);
    }

    /**
     * El broker no respondio dentro del tiempo limite. Las causas habituales
     * son que el contenedor no esta levantado o que el topic no existe.
     */
    @ExceptionHandler(TimeoutException.class)
    public ResponseEntity<Map<String, Object>> tiempoAgotado(TimeoutException e) {
        log.error("Tiempo agotado al comunicarse con Kafka", e);
        return respuesta(HttpStatus.SERVICE_UNAVAILABLE,
                "El broker no respondio dentro del tiempo limite. Verifique que el contenedor "
                        + "este en ejecucion y que el topic exista.", null);
    }

    /** Fallo al consultar metadatos del cluster. */
    @ExceptionHandler(ConsultaKafkaException.class)
    public ResponseEntity<Map<String, Object>> consultaFallida(ConsultaKafkaException e) {
        log.error("Fallo una consulta de metadatos a Kafka", e);
        return respuesta(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage(), null);
    }

    /**
     * Excepciones que ya traen su propio codigo HTTP, como un 404 de ruta
     * inexistente. Se atienden aqui para que la red de seguridad de mas abajo
     * no las convierta en 500.
     */
    @ExceptionHandler(ErrorResponseException.class)
    public ResponseEntity<Map<String, Object>> errorConEstadoPropio(ErrorResponseException e) {
        HttpStatus estado = HttpStatus.valueOf(e.getStatusCode().value());
        return respuesta(estado, e.getMessage(), null);
    }

    /** Red de seguridad para cualquier fallo no previsto. */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> errorInesperado(Exception e) {
        log.error("Error inesperado atendiendo la peticion", e);
        return respuesta(HttpStatus.INTERNAL_SERVER_ERROR,
                "Error inesperado: " + e.getClass().getSimpleName(), null);
    }

    /** Arma el cuerpo de la respuesta de error con una forma estable. */
    private ResponseEntity<Map<String, Object>> respuesta(HttpStatus estado,
                                                          String mensaje,
                                                          Map<String, String> detalles) {
        Map<String, Object> cuerpo = new LinkedHashMap<>();
        cuerpo.put("estado", estado.value());
        cuerpo.put("error", estado.getReasonPhrase());
        cuerpo.put("mensaje", mensaje);
        if (detalles != null && !detalles.isEmpty()) {
            cuerpo.put("detalles", detalles);
        }
        return ResponseEntity.status(estado).body(cuerpo);
    }
}
