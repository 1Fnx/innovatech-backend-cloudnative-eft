package cl.innovatech.kafka.controller;

import cl.innovatech.kafka.dto.EstadoCluster;
import cl.innovatech.kafka.dto.ResultadoAdministracion;
import cl.innovatech.kafka.dto.ResumenGrupo;
import cl.innovatech.kafka.dto.ResumenTopic;
import cl.innovatech.kafka.dto.SolicitudConfiguracion;
import cl.innovatech.kafka.dto.SolicitudParticiones;
import cl.innovatech.kafka.dto.SolicitudTopic;
import cl.innovatech.kafka.service.AdministracionTopicsService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * API de administracion del cluster.
 *
 *   GET    /cluster                        brokers disponibles y controlador
 *   GET    /topics                         listado con particiones y config
 *   GET    /topics/{n}                     ficha de un topico
 *   POST   /topics                         crear (y opcionalmente su .DLT)
 *   PATCH  /topics/{n}/particiones         ampliar particiones
 *   PATCH  /topics/{n}/configuracion       retencion, politicas y similares
 *   DELETE /topics/{n}                     eliminar
 *   GET    /grupos                         grupos consumidores y su retraso
 *
 * PATCH y no PUT en las dos modificaciones: se aplica un subconjunto de la
 * configuracion, no se reemplaza el recurso completo.
 */
@RestController
@RequestMapping("/api/kafka/admin")
@Validated
public class AdministracionKafkaController {

    /** Caracteres admitidos por Kafka en el nombre de un topico. */
    private static final String PATRON_NOMBRE = "^[A-Za-z0-9._\\-]+$";

    private final AdministracionTopicsService administracion;

    public AdministracionKafkaController(AdministracionTopicsService administracion) {
        this.administracion = administracion;
    }

    @GetMapping("/cluster")
    public EstadoCluster cluster() {
        return administracion.describirCluster();
    }

    /**
     * @param incluirInternos true para incluir los topicos de uso interno del
     *                        cluster, como __consumer_offsets
     */
    @GetMapping("/topics")
    public List<ResumenTopic> topics(
            @RequestParam(defaultValue = "false") boolean incluirInternos) {
        return administracion.listarTopics(incluirInternos);
    }

    @GetMapping("/topics/{nombre}")
    public ResumenTopic topic(
            @PathVariable
            @NotBlank
            @Pattern(regexp = PATRON_NOMBRE,
                     message = "El nombre solo admite letras, numeros, punto, guion y guion bajo")
            String nombre) {
        return administracion.describirTopic(nombre);
    }

    /** Responde 201; el cuerpo informa que se creo y con que forma. */
    @PostMapping("/topics")
    public ResponseEntity<ResultadoAdministracion> crear(
            @Valid @RequestBody SolicitudTopic solicitud) {
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(administracion.crearTopic(solicitud));
    }

    /** Solo admite aumentar: Kafka no permite reducir particiones. */
    @PatchMapping("/topics/{nombre}/particiones")
    public ResultadoAdministracion ampliarParticiones(
            @PathVariable
            @NotBlank
            @Pattern(regexp = PATRON_NOMBRE,
                     message = "El nombre solo admite letras, numeros, punto, guion y guion bajo")
            String nombre,
            @Valid @RequestBody SolicitudParticiones solicitud) {
        return administracion.ampliarParticiones(nombre, solicitud);
    }

    /** Aplica solo las claves enviadas; el resto de la configuracion no cambia. */
    @PatchMapping("/topics/{nombre}/configuracion")
    public ResultadoAdministracion configurar(
            @PathVariable
            @NotBlank
            @Pattern(regexp = PATRON_NOMBRE,
                     message = "El nombre solo admite letras, numeros, punto, guion y guion bajo")
            String nombre,
            @Valid @RequestBody SolicitudConfiguracion solicitud) {
        return administracion.configurarTopic(nombre, solicitud);
    }

    /** Los topicos del flujo de pedidos exigen confirmar=true. */
    @DeleteMapping("/topics/{nombre}")
    public ResultadoAdministracion eliminar(
            @PathVariable
            @NotBlank
            @Pattern(regexp = PATRON_NOMBRE,
                     message = "El nombre solo admite letras, numeros, punto, guion y guion bajo")
            String nombre,
            @RequestParam(defaultValue = "false") boolean confirmar) {
        return administracion.eliminarTopic(nombre, confirmar);
    }

    @GetMapping("/grupos")
    public List<ResumenGrupo> grupos() {
        return administracion.listarGrupos();
    }
}
