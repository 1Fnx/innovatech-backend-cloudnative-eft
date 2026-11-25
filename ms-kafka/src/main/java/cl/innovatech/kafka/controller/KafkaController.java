package cl.innovatech.kafka.controller;

import cl.innovatech.kafka.config.KafkaProperties;
import cl.innovatech.kafka.dto.DescripcionTopic;
import cl.innovatech.kafka.dto.EstadoGrupo;
import cl.innovatech.kafka.dto.RegistroRecibido;
import cl.innovatech.kafka.dto.ResultadoPublicacion;
import cl.innovatech.kafka.service.BitacoraConsumoService;
import cl.innovatech.kafka.service.ObservabilidadKafkaService;
import cl.innovatech.kafka.service.ProductorService;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * API REST del microservicio de Kafka.
 *
 * El controlador se limita a recibir la peticion, validar los parametros y
 * delegar: publicar es tarea de ProductorService y consultar metadatos es tarea
 * de ObservabilidadKafkaService. Aqui no se instancia KafkaTemplate ni
 * AdminClient, de modo que la capa web queda separada de la de mensajeria.
 *
 * Endpoints:
 *   GET  /api/kafka/health      comprobacion de vida del servicio
 *   POST /api/kafka/send        publica un registro, con clave opcional
 *   GET  /api/kafka/recibidos   registros que leyo el consumidor
 *   GET  /api/kafka/topico      descripcion del topic y sus particiones
 *   GET  /api/kafka/grupo       avance del grupo consumidor (offsets y retraso)
 *   POST /api/kafka/recibidos/limpiar  vacia la ventana en memoria
 *
 * Las operaciones de administracion viven aparte, en
 * AdministracionKafkaController, bajo /api/kafka/admin.
 */
@RestController
@RequestMapping("/api/kafka")
@Validated
public class KafkaController {

    private final ProductorService productor;
    private final BitacoraConsumoService bitacora;
    private final ObservabilidadKafkaService observabilidad;
    private final KafkaProperties propiedades;

    public KafkaController(ProductorService productor,
                           BitacoraConsumoService bitacora,
                           ObservabilidadKafkaService observabilidad,
                           KafkaProperties propiedades) {
        this.productor = productor;
        this.bitacora = bitacora;
        this.observabilidad = observabilidad;
        this.propiedades = propiedades;
    }

    /**
     * Comprobacion de vida.
     *
     * Responde sin consultar al broker: informa que la aplicacion Spring Boot
     * esta atendiendo peticiones. Para saber si el cluster responde hay que
     * usar /api/kafka/topico, que si lo consulta.
     */
    @GetMapping("/health")
    public Map<String, String> salud() {
        Map<String, String> estado = new LinkedHashMap<>();
        estado.put("status", "ok");
        estado.put("servicio", "ms-kafka");
        estado.put("mensaje", "API Kafka operativa");
        estado.put("puerto", "8888");
        estado.put("topicLaboratorio", propiedades.getTopic());
        estado.put("grupoLaboratorio", propiedades.getGrupoId());
        estado.put("topicsDelNegocio", String.join(", ", propiedades.getTopicsDelNegocio()));
        estado.put("gruposDelNegocio", String.join(", ", propiedades.getGruposDelNegocio()));
        estado.put("administracion", "/api/kafka/admin");
        return estado;
    }

    /**
     * Publica un registro en el topic.
     *
     * Se usa POST y no GET porque la operacion produce un efecto: escribe un
     * registro en el log del broker. Un GET deberia poder repetirse sin
     * consecuencias, y aqui cada llamada agrega un registro nuevo.
     *
     * La clave es opcional, y de ella depende el comportamiento observado:
     *   sin clave : el particionador por defecto elige la particion sin
     *               considerar ninguna clave. Puede dejar varios registros
     *               consecutivos en la misma particion, por lo que no se debe
     *               esperar necesariamente el reparto 0, 1, 2.
     *   con clave : la particion sale del hash de la clave, de modo que todos
     *               los registros que comparten clave caen en la misma
     *               particion y conservan su orden relativo en el log.
     *
     * Los parametros se validan antes de publicar: un mensaje vacio o una clave
     * con caracteres no previstos se rechazan con 400 y nunca llegan al broker.
     */
    @PostMapping("/send")
    public ResponseEntity<ResultadoPublicacion> publicar(

            @RequestParam
            @NotBlank(message = "El mensaje no puede estar vacio")
            @Size(max = 500, message = "El mensaje no puede superar los 500 caracteres")
            String message,

            @RequestParam(required = false)
            @Size(max = 100, message = "La clave no puede superar los 100 caracteres")
            @Pattern(regexp = "^[A-Za-z0-9._\\-]*$",
                     message = "La clave solo admite letras, numeros, punto, guion y guion bajo")
            String key) throws Exception {

        return ResponseEntity.ok(productor.publicar(message, key));
    }

    /**
     * Registros que el consumidor alcanzo a leer, del mas reciente al mas
     * antiguo.
     *
     * Es la misma informacion que aparece en la consola del servidor. El log
     * autoritativo sigue estando en Kafka: esto es solo una ventana de los
     * ultimos registros, pensada para poder observarlos desde el navegador.
     */
    @GetMapping("/recibidos")
    public List<RegistroRecibido> recibidos() {
        return bitacora.listar();
    }

    /** Vacia la ventana en memoria. No borra nada en Kafka ni mueve offsets. */
    @PostMapping("/recibidos/limpiar")
    public Map<String, Object> limpiarRecibidos() {
        int cantidad = bitacora.limpiar();
        return Map.of("eliminados", cantidad,
                "aviso", "Solo se limpio la vista en memoria. El log de Kafka no se modifica.");
    }

    /**
     * Descripcion del topic: particiones, factor de replicacion, lider de cada
     * particion y avance de su log. Equivale a kafka-topics.sh --describe.
     */
    @GetMapping("/topico")
    public DescripcionTopic topico(
            @RequestParam(required = false)
            @Size(max = 200, message = "El nombre no puede superar los 200 caracteres")
            @Pattern(regexp = "^[A-Za-z0-9._\\-]*$",
                     message = "El nombre solo admite letras, numeros, punto, guion y guion bajo")
            String topic) {

        return (topic == null || topic.isBlank())
                ? observabilidad.describirTopic()
                : observabilidad.describirTopic(topic.trim());
    }

    /**
     * Avance del grupo consumidor por particion: offset confirmado, final del
     * log y retraso. Equivale a kafka-consumer-groups.sh --describe --group.
     */
    @GetMapping("/grupo")
    public EstadoGrupo grupo(
            @RequestParam(required = false)
            @Size(max = 200, message = "El nombre del grupo no puede superar los 200 caracteres")
            @Pattern(regexp = "^[A-Za-z0-9._\\-]*$",
                     message = "El grupo solo admite letras, numeros, punto, guion y guion bajo")
            String grupo,

            @RequestParam(required = false)
            @Size(max = 200, message = "El nombre del topic no puede superar los 200 caracteres")
            @Pattern(regexp = "^[A-Za-z0-9._\\-]*$",
                     message = "El topic solo admite letras, numeros, punto, guion y guion bajo")
            String topic) {

        boolean sinGrupo = grupo == null || grupo.isBlank();
        boolean sinTopic = topic == null || topic.isBlank();

        if (sinGrupo && sinTopic) {
            return observabilidad.describirGrupo();
        }
        // Si solo viene uno, el otro toma el valor del topico de pruebas.
        return observabilidad.describirGrupo(
                sinGrupo ? propiedades.getGrupoId() : grupo.trim(),
                sinTopic ? propiedades.getTopic() : topic.trim());
    }
}
