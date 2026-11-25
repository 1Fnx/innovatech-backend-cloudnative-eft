package cl.innovatech.adminrabbit.service;

import cl.innovatech.adminrabbit.dto.*;
import cl.innovatech.adminrabbit.exception.OperacionRabbitException;
import cl.innovatech.adminrabbit.exception.RecursoNoEncontradoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Servicio dedicado a la administracion de recursos de RabbitMQ.
 *
 * Toda la creacion, consulta y eliminacion de colas, exchanges y bindings vive
 * aqui. El controlador solo invoca metodos de alto nivel y no conoce
 * RabbitAdmin, QueueBuilder ni los argumentos "x-" del protocolo AMQP.
 */
@Service
public class RabbitAdminService {

    private static final Logger log = LoggerFactory.getLogger(RabbitAdminService.class);

    private final RabbitAdmin rabbitAdmin;

    public RabbitAdminService(RabbitAdmin rabbitAdmin) {
        this.rabbitAdmin = rabbitAdmin;
    }

    // =========================== COLAS ===========================

    /** Crea una cola con sus argumentos opcionales (DLX, TTL y longitud maxima). */
    public ColaInfoResponse crearCola(CrearColaRequest solicitud) {
        try {
            Map<String, Object> argumentos = new HashMap<>();

            if (textoPresente(solicitud.deadLetterExchange())) {
                argumentos.put("x-dead-letter-exchange", solicitud.deadLetterExchange());
            }
            if (textoPresente(solicitud.deadLetterRoutingKey())) {
                argumentos.put("x-dead-letter-routing-key", solicitud.deadLetterRoutingKey());
            }
            if (solicitud.mensajeTtlMs() != null) {
                argumentos.put("x-message-ttl", solicitud.mensajeTtlMs());
            }
            if (solicitud.longitudMaxima() != null) {
                argumentos.put("x-max-length", solicitud.longitudMaxima());
            }

            Queue cola = new Queue(solicitud.nombre(), solicitud.esDurable(),
                    false, solicitud.esAutoDelete(), argumentos);

            rabbitAdmin.declareQueue(cola);
            log.info("[ADMIN] Cola creada. nombre={} durable={} autoDelete={} argumentos={}",
                    solicitud.nombre(), solicitud.esDurable(), solicitud.esAutoDelete(), argumentos);

            return obtenerInfoCola(solicitud.nombre());

        } catch (Exception e) {
            log.error("[ADMIN] No se pudo crear la cola '{}': {}", solicitud.nombre(), e.getMessage());
            throw new OperacionRabbitException(
                    "No se pudo crear la cola '" + solicitud.nombre() + "'. " + causaLegible(e), e);
        }
    }

    /** Consulta el estado de una cola: cuantos mensajes y consumidores tiene. */
    public ColaInfoResponse obtenerInfoCola(String nombre) {
        QueueInformation info = rabbitAdmin.getQueueInfo(nombre);
        if (info == null) {
            throw new RecursoNoEncontradoException("La cola '" + nombre + "' no existe en el broker");
        }
        return new ColaInfoResponse(info.getName(), info.getMessageCount(), info.getConsumerCount());
    }

    /** Elimina una cola. Falla con 404 si la cola no existe. */
    public void eliminarCola(String nombre) {
        if (rabbitAdmin.getQueueInfo(nombre) == null) {
            throw new RecursoNoEncontradoException("La cola '" + nombre + "' no existe en el broker");
        }
        boolean eliminada = rabbitAdmin.deleteQueue(nombre);
        log.info("[ADMIN] Cola eliminada. nombre={} resultado={}", nombre, eliminada);
        if (!eliminada) {
            throw new OperacionRabbitException("No se pudo eliminar la cola '" + nombre + "'", null);
        }
    }

    /** Vacia el contenido de una cola sin eliminarla. */
    public int purgarCola(String nombre) {
        if (rabbitAdmin.getQueueInfo(nombre) == null) {
            throw new RecursoNoEncontradoException("La cola '" + nombre + "' no existe en el broker");
        }
        int total = rabbitAdmin.purgeQueue(nombre);
        log.info("[ADMIN] Cola purgada. nombre={} mensajesEliminados={}", nombre, total);
        return total;
    }

    // ========================= EXCHANGES =========================

    /** Crea un exchange del tipo indicado (direct, topic, fanout o headers). */
    public void crearExchange(CrearExchangeRequest solicitud) {
        try {
            Exchange exchange = construirExchange(solicitud);
            rabbitAdmin.declareExchange(exchange);
            log.info("[ADMIN] Exchange creado. nombre={} tipo={} durable={}",
                    solicitud.nombre(), solicitud.tipo(), solicitud.esDurable());
        } catch (Exception e) {
            log.error("[ADMIN] No se pudo crear el exchange '{}': {}", solicitud.nombre(), e.getMessage());
            throw new OperacionRabbitException(
                    "No se pudo crear el exchange '" + solicitud.nombre() + "'. " + causaLegible(e), e);
        }
    }

    public void eliminarExchange(String nombre) {
        boolean eliminado = rabbitAdmin.deleteExchange(nombre);
        log.info("[ADMIN] Exchange eliminado. nombre={} resultado={}", nombre, eliminado);
        if (!eliminado) {
            throw new RecursoNoEncontradoException(
                    "El exchange '" + nombre + "' no existe o no pudo eliminarse");
        }
    }

    private Exchange construirExchange(CrearExchangeRequest s) {
        return switch (s.tipo()) {
            case "direct"  -> new DirectExchange(s.nombre(), s.esDurable(), s.esAutoDelete());
            case "topic"   -> new TopicExchange(s.nombre(), s.esDurable(), s.esAutoDelete());
            case "fanout"  -> new FanoutExchange(s.nombre(), s.esDurable(), s.esAutoDelete());
            case "headers" -> new HeadersExchange(s.nombre(), s.esDurable(), s.esAutoDelete());
            default -> throw new IllegalArgumentException("Tipo de exchange no soportado: " + s.tipo());
        };
    }

    // ========================== BINDINGS =========================

    /** Enlaza una cola existente con un exchange mediante una routing key. */
    public void crearBinding(CrearBindingRequest solicitud) {
        try {
            rabbitAdmin.declareBinding(construirBinding(solicitud));
            log.info("[ADMIN] Binding creado. cola={} exchange={} routingKey='{}'",
                    solicitud.cola(), solicitud.exchange(), solicitud.routingKey());
        } catch (Exception e) {
            log.error("[ADMIN] No se pudo crear el binding: {}", e.getMessage());
            throw new OperacionRabbitException("No se pudo crear el binding. " + causaLegible(e), e);
        }
    }

    public void eliminarBinding(CrearBindingRequest solicitud) {
        try {
            rabbitAdmin.removeBinding(construirBinding(solicitud));
            log.info("[ADMIN] Binding eliminado. cola={} exchange={} routingKey='{}'",
                    solicitud.cola(), solicitud.exchange(), solicitud.routingKey());
        } catch (Exception e) {
            log.error("[ADMIN] No se pudo eliminar el binding: {}", e.getMessage());
            throw new OperacionRabbitException("No se pudo eliminar el binding. " + causaLegible(e), e);
        }
    }

    private Binding construirBinding(CrearBindingRequest s) {
        return new Binding(s.cola(), Binding.DestinationType.QUEUE,
                s.exchange(), s.routingKey(), null);
    }

    // ========================== AUXILIARES =======================

    private boolean textoPresente(String valor) {
        return valor != null && !valor.isBlank();
    }

    private String causaLegible(Exception e) {
        Throwable raiz = e;
        while (raiz.getCause() != null) raiz = raiz.getCause();
        return raiz.getMessage() == null ? e.getClass().getSimpleName() : raiz.getMessage();
    }
}
