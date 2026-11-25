package cl.innovatech.notificaciones.controller;

import cl.innovatech.notificaciones.domain.RegistroProceso;
import cl.innovatech.notificaciones.service.RegistroProcesoService;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * API de consulta del microservicio de Notificaciones.
 * Permite comprobar desde el frontend que el trabajo asincrono se ejecuto y con
 * que resultado, sin necesidad de mirar la consola del servidor.
 */
@RestController
@RequestMapping("/api")
public class ConsultaController {

    private final RegistroProcesoService bitacora;

    public ConsultaController(RegistroProcesoService bitacora) {
        this.bitacora = bitacora;
    }

    @GetMapping("/health")
    public Map<String, String> salud() {
        return Map.of("status", "ok",
                "servicio", "ms-notificaciones",
                "mensaje", "Microservicio de Notificaciones en ejecucion.");
    }

    /** Ultimos mensajes procesados, con su dominio y resultado. */
    @GetMapping("/procesos")
    @PreAuthorize("hasAuthority('SCOPE_Productos.Read')")
    public List<RegistroProceso> procesos() {
        return bitacora.ultimos();
    }
}
