package cl.innovatech.productos.controller;

import cl.innovatech.productos.domain.Producto;
import cl.innovatech.productos.dto.ProductoRequest;
import cl.innovatech.productos.service.ProductoService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** API REST del catalogo de productos, protegida por ambitos y roles del JWT. */
@RestController
@RequestMapping("/api")
public class ProductoController {

    private final ProductoService servicio;

    public ProductoController(ProductoService servicio) {
        this.servicio = servicio;
    }

    @GetMapping("/health")
    public Map<String, String> salud() {
        return Map.of("status", "ok",
                "servicio", "ms-productos",
                "mensaje", "Microservicio de Productos en ejecucion.");
    }

    /** Devuelve los claims que el backend leyo del token recibido. */
    @GetMapping("/me")
    @PreAuthorize("isAuthenticated()")
    public Map<String, Object> miPerfil(@AuthenticationPrincipal Jwt jwt) {
        Map<String, Object> datos = new LinkedHashMap<>();
        datos.put("sub", jwt.getSubject());
        datos.put("name", jwt.getClaimAsString("name"));
        datos.put("aud", jwt.getAudience());
        datos.put("iss", jwt.getIssuer().toString());
        datos.put("scp", jwt.getClaimAsString("scp"));
        datos.put("roles", jwt.getClaimAsStringList("roles"));
        datos.put("exp", jwt.getExpiresAt());
        return datos;
    }

    @GetMapping("/productos")
    @PreAuthorize("hasAuthority('SCOPE_Productos.Read')")
    public List<Producto> listar() { return servicio.listar(); }

    @GetMapping("/productos/{id}")
    @PreAuthorize("hasAuthority('SCOPE_Productos.Read')")
    public ResponseEntity<Producto> obtener(@PathVariable Long id) {
        Producto p = servicio.obtener(id);
        return p == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(p);
    }

    @PostMapping("/productos")
    @PreAuthorize("hasAuthority('SCOPE_Productos.Write')")
    public ResponseEntity<Producto> crear(@Valid @RequestBody ProductoRequest datos) {
        return ResponseEntity.status(HttpStatus.CREATED).body(servicio.crear(datos));
    }

    @PutMapping("/productos/{id}")
    @PreAuthorize("hasAuthority('SCOPE_Productos.Write')")
    public ResponseEntity<Producto> actualizar(@PathVariable Long id,
                                               @Valid @RequestBody ProductoRequest datos) {
        Producto p = servicio.actualizar(id, datos);
        return p == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(p);
    }

    @DeleteMapping("/productos/{id}")
    @PreAuthorize("hasAuthority('SCOPE_Productos.Write')")
    public ResponseEntity<Void> eliminar(@PathVariable Long id) {
        return servicio.eliminar(id)
                ? ResponseEntity.noContent().build()
                : ResponseEntity.notFound().build();
    }

    /** Endpoint restringido al rol Admin: el resto recibe 403. */
    @GetMapping("/productos/admin/resumen")
    @PreAuthorize("hasRole('Admin')")
    public Map<String, Object> resumenAdmin() {
        List<Producto> productos = servicio.listar();
        return Map.of(
                "totalProductos", productos.size(),
                "stockTotal", productos.stream().mapToInt(Producto::getStock).sum());
    }
}
