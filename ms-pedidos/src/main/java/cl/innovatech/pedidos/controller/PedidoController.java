package cl.innovatech.pedidos.controller;

import cl.innovatech.pedidos.domain.Pedido;
import cl.innovatech.pedidos.dto.CrearPedidoRequest;
import cl.innovatech.pedidos.service.PedidoService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * API REST de pedidos.
 *
 * Cada endpoint exige el ambito correspondiente del token emitido por el IDaaS.
 * El controlador no conoce RabbitMQ: delega en el servicio de negocio.
 */
@RestController
@RequestMapping("/api")
public class PedidoController {

    private final PedidoService servicio;

    public PedidoController(PedidoService servicio) {
        this.servicio = servicio;
    }

    /** Endpoint publico para comprobar que el microservicio esta operativo. */
    @GetMapping("/health")
    public Map<String, String> salud() {
        return Map.of("status", "ok",
                "servicio", "ms-pedidos",
                "mensaje", "Microservicio de Pedidos en ejecucion.");
    }

    @GetMapping("/pedidos")
    @PreAuthorize("hasAuthority('SCOPE_Productos.Read')")
    public List<Pedido> listar() {
        return servicio.listar();
    }

    @GetMapping("/pedidos/{id}")
    @PreAuthorize("hasAuthority('SCOPE_Productos.Read')")
    public ResponseEntity<Pedido> obtener(@PathVariable Long id) {
        Pedido pedido = servicio.obtener(id);
        return pedido == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(pedido);
    }

    /**
     * Crea un pedido y publica el evento correspondiente en RabbitMQ.
     * Responde 201 apenas el pedido queda guardado: el resto del trabajo
     * (notificacion, boleta e inventario) ocurre de forma asincrona.
     */
    @PostMapping("/pedidos")
    @PreAuthorize("hasAuthority('SCOPE_Productos.Write')")
    public ResponseEntity<Pedido> crear(@Valid @RequestBody CrearPedidoRequest solicitud) {
        return ResponseEntity.status(HttpStatus.CREATED).body(servicio.crear(solicitud));
    }
}
