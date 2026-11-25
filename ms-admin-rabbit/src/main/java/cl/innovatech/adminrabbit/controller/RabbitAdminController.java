package cl.innovatech.adminrabbit.controller;

import cl.innovatech.adminrabbit.config.MensajeriaProperties;
import cl.innovatech.adminrabbit.dto.*;
import cl.innovatech.adminrabbit.service.RabbitAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * API REST del microservicio administrador de RabbitMQ.
 *
 * Mapa de rutas expuestas:
 *
 *   POST   /api/rabbit/queues                 -> crear una cola
 *   GET    /api/rabbit/queues/{nombre}        -> consultar el estado de una cola
 *   DELETE /api/rabbit/queues/{nombre}        -> eliminar una cola
 *   POST   /api/rabbit/queues/{nombre}/purge  -> vaciar una cola
 *   POST   /api/rabbit/exchanges              -> crear un exchange
 *   DELETE /api/rabbit/exchanges/{nombre}     -> eliminar un exchange
 *   POST   /api/rabbit/bindings               -> crear un binding
 *   DELETE /api/rabbit/bindings               -> eliminar un binding
 *   GET    /api/rabbit/topologia              -> nombres definidos de forma centralizada
 *
 * Cada recurso usa su sustantivo en plural y el verbo HTTP expresa la accion.
 * El controlador solo traduce HTTP y delega: no contiene logica de RabbitMQ.
 */
@RestController
@RequestMapping("/api")
public class RabbitAdminController {

    private final RabbitAdminService servicio;
    private final MensajeriaProperties propiedades;

    public RabbitAdminController(RabbitAdminService servicio, MensajeriaProperties propiedades) {
        this.servicio = servicio;
        this.propiedades = propiedades;
    }

    @GetMapping("/health")
    public Map<String, String> salud() {
        return Map.of("status", "ok",
                "servicio", "ms-admin-rabbit",
                "mensaje", "Microservicio administrador de RabbitMQ en ejecucion.");
    }

    /** Devuelve la topologia declarada de forma centralizada en application.yml. */
    @GetMapping("/rabbit/topologia")
    @PreAuthorize("hasAuthority('SCOPE_Productos.Read')")
    public Map<String, Object> topologia() {
        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("exchanges", Map.of(
                "pedidos", propiedades.getExchanges().getPedidos(),
                "dlx", propiedades.getExchanges().getDlx()));
        respuesta.put("routingKeys", Map.of(
                "pedidoCreado", propiedades.getRoutingKeys().getPedidoCreado(),
                "dlqNotificaciones", propiedades.getRoutingKeys().getDlqNotificaciones(),
                "dlqDocumentos", propiedades.getRoutingKeys().getDlqDocumentos(),
                "dlqInventario", propiedades.getRoutingKeys().getDlqInventario()));
        respuesta.put("queues", Map.of(
                "notificaciones", propiedades.getQueues().getNotificaciones(),
                "documentos", propiedades.getQueues().getDocumentos(),
                "inventario", propiedades.getQueues().getInventario(),
                "notificacionesDlq", propiedades.getQueues().getNotificacionesDlq(),
                "documentosDlq", propiedades.getQueues().getDocumentosDlq(),
                "inventarioDlq", propiedades.getQueues().getInventarioDlq()));
        return respuesta;
    }

    // ----------------------------- Colas ------------------------------------

    @PostMapping("/rabbit/queues")
    @PreAuthorize("hasAuthority('SCOPE_Productos.Write')")
    public ResponseEntity<ColaInfoResponse> crearCola(@Valid @RequestBody CrearColaRequest solicitud) {
        return ResponseEntity.status(HttpStatus.CREATED).body(servicio.crearCola(solicitud));
    }

    @GetMapping("/rabbit/queues/{nombre}")
    @PreAuthorize("hasAuthority('SCOPE_Productos.Read')")
    public ColaInfoResponse consultarCola(@PathVariable String nombre) {
        return servicio.obtenerInfoCola(nombre);
    }

    /** Operacion destructiva: se exige ademas el rol Admin del token. */
    @DeleteMapping("/rabbit/queues/{nombre}")
    @PreAuthorize("hasRole('Admin')")
    public ResponseEntity<Map<String, String>> eliminarCola(@PathVariable String nombre) {
        servicio.eliminarCola(nombre);
        return ResponseEntity.ok(Map.of("mensaje", "Cola '" + nombre + "' eliminada correctamente"));
    }

    @PostMapping("/rabbit/queues/{nombre}/purge")
    @PreAuthorize("hasRole('Admin')")
    public Map<String, Object> purgarCola(@PathVariable String nombre) {
        int eliminados = servicio.purgarCola(nombre);
        return Map.of("cola", nombre, "mensajesEliminados", eliminados);
    }

    // --------------------------- Exchanges ----------------------------------

    @PostMapping("/rabbit/exchanges")
    @PreAuthorize("hasAuthority('SCOPE_Productos.Write')")
    public ResponseEntity<Map<String, String>> crearExchange(
            @Valid @RequestBody CrearExchangeRequest solicitud) {
        servicio.crearExchange(solicitud);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "mensaje", "Exchange '" + solicitud.nombre() + "' creado correctamente",
                "tipo", solicitud.tipo()));
    }

    @DeleteMapping("/rabbit/exchanges/{nombre}")
    @PreAuthorize("hasRole('Admin')")
    public Map<String, String> eliminarExchange(@PathVariable String nombre) {
        servicio.eliminarExchange(nombre);
        return Map.of("mensaje", "Exchange '" + nombre + "' eliminado correctamente");
    }

    // ---------------------------- Bindings ----------------------------------

    @PostMapping("/rabbit/bindings")
    @PreAuthorize("hasAuthority('SCOPE_Productos.Write')")
    public ResponseEntity<Map<String, String>> crearBinding(
            @Valid @RequestBody CrearBindingRequest solicitud) {
        servicio.crearBinding(solicitud);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "mensaje", "Binding creado: " + solicitud.exchange()
                        + " --(" + solicitud.routingKey() + ")--> " + solicitud.cola()));
    }

    @DeleteMapping("/rabbit/bindings")
    @PreAuthorize("hasRole('Admin')")
    public Map<String, String> eliminarBinding(@Valid @RequestBody CrearBindingRequest solicitud) {
        servicio.eliminarBinding(solicitud);
        return Map.of("mensaje", "Binding eliminado entre '" + solicitud.exchange()
                + "' y '" + solicitud.cola() + "'");
    }
}
