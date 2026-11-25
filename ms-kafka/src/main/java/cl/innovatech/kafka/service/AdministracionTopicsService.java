package cl.innovatech.kafka.service;

import cl.innovatech.kafka.config.KafkaProperties;
import cl.innovatech.kafka.dto.EstadoCluster;
import cl.innovatech.kafka.dto.ResultadoAdministracion;
import cl.innovatech.kafka.dto.ResumenGrupo;
import cl.innovatech.kafka.dto.ResumenTopic;
import cl.innovatech.kafka.dto.SolicitudConfiguracion;
import cl.innovatech.kafka.dto.SolicitudParticiones;
import cl.innovatech.kafka.dto.SolicitudTopic;
import cl.innovatech.kafka.exception.ConsultaKafkaException;
import cl.innovatech.kafka.exception.OperacionKafkaException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Operaciones de administracion del cluster sobre AdminClient.
 *
 * Equivale por HTTP a los scripts que vienen en el contenedor del broker:
 * kafka-topics.sh, kafka-configs.sh y kafka-consumer-groups.sh. Se expone por
 * HTTP para no depender de acceso al contenedor y para aplicar las mismas
 * validaciones que el resto de la API.
 *
 * Restricciones que impone esta capa, por encima de lo que permite AdminClient:
 *   - Los topicos internos del cluster (prefijo __) no se eliminan: borrar
 *     __consumer_offsets dejaria a todos los grupos sin su registro de avance.
 *   - Los topicos del negocio exigen confirmacion explicita para eliminarse.
 *   - Las particiones solo se amplian; Kafka no admite reducirlas.
 */
@Service
public class AdministracionTopicsService {

    private static final Logger log = LoggerFactory.getLogger(AdministracionTopicsService.class);

    /** Tiempo limite de cada operacion contra el cluster, en segundos. */
    private static final int LIMITE_SEGUNDOS = 15;

    /** Prefijo de los topicos de uso interno del cluster. */
    private static final String PREFIJO_INTERNO = "__";

    private final AdminClient admin;
    private final KafkaProperties propiedades;

    public AdministracionTopicsService(AdminClient admin, KafkaProperties propiedades) {
        this.admin = admin;
        this.propiedades = propiedades;
    }

    // =========================================================================
    //  Cluster
    // =========================================================================

    /** Brokers disponibles y broker controlador. */
    public EstadoCluster describirCluster() {
        try {
            DescribeClusterResult resultado = admin.describeCluster();

            String id = resultado.clusterId().get(LIMITE_SEGUNDOS, TimeUnit.SECONDS);
            Node coordinador = resultado.controller().get(LIMITE_SEGUNDOS, TimeUnit.SECONDS);
            Collection<Node> nodos = resultado.nodes().get(LIMITE_SEGUNDOS, TimeUnit.SECONDS);

            List<EstadoCluster.BrokerInfo> brokers = new ArrayList<>();
            for (Node nodo : nodos) {
                brokers.add(new EstadoCluster.BrokerInfo(nodo.id(), nodo.host(), nodo.port()));
            }
            brokers.sort(Comparator.comparingInt(EstadoCluster.BrokerInfo::id));

            return new EstadoCluster(
                    id,
                    coordinador == null ? -1 : coordinador.id(),
                    brokers,
                    brokers.size());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConsultaKafkaException("Consulta interrumpida al describir el cluster", e);
        } catch (Exception e) {
            throw new ConsultaKafkaException(
                    "No fue posible describir el cluster. Verifique que los brokers esten en ejecucion.", e);
        }
    }

    // =========================================================================
    //  Consulta de topicos
    // =========================================================================

    /**
     * Topicos con su detalle de particiones y su configuracion explicita.
     *
     * @param incluirInternos true para incluir los topicos de uso interno del
     *                        cluster, como __consumer_offsets
     */
    public List<ResumenTopic> listarTopics(boolean incluirInternos) {
        try {
            Collection<TopicListing> listado = admin.listTopics()
                    .listings()
                    .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS);

            Set<String> nombres = new TreeSet<>();
            for (TopicListing listing : listado) {
                boolean interno = listing.isInternal() || listing.name().startsWith(PREFIJO_INTERNO);
                if (interno && !incluirInternos) {
                    continue;
                }
                nombres.add(listing.name());
            }

            if (nombres.isEmpty()) {
                return List.of();
            }

            Map<String, TopicDescription> descripciones = admin.describeTopics(nombres)
                    .allTopicNames()
                    .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS);

            Map<String, Map<String, String>> configuraciones = leerConfiguraciones(nombres);

            List<ResumenTopic> resumenes = new ArrayList<>();
            for (String nombre : nombres) {
                TopicDescription descripcion = descripciones.get(nombre);
                if (descripcion == null) {
                    continue;
                }
                resumenes.add(armarResumen(descripcion,
                        configuraciones.getOrDefault(nombre, Map.of())));
            }
            return resumenes;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConsultaKafkaException("Consulta interrumpida al listar los topics", e);
        } catch (Exception e) {
            throw new ConsultaKafkaException(
                    "No fue posible listar los topics. Verifique que el cluster este en ejecucion.", e);
        }
    }

    /** Ficha de un topico. Lanza OperacionKafkaException si no existe. */
    public ResumenTopic describirTopic(String nombre) {
        try {
            TopicDescription descripcion = admin.describeTopics(List.of(nombre))
                    .allTopicNames()
                    .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS)
                    .get(nombre);

            return armarResumen(descripcion,
                    leerConfiguraciones(Set.of(nombre)).getOrDefault(nombre, Map.of()));

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConsultaKafkaException("Consulta interrumpida al describir " + nombre, e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                throw new OperacionKafkaException("El topic " + nombre + " no existe en el cluster");
            }
            throw new ConsultaKafkaException("No fue posible describir el topic " + nombre, e);
        } catch (Exception e) {
            throw new ConsultaKafkaException("No fue posible describir el topic " + nombre, e);
        }
    }

    // =========================================================================
    //  Creacion
    // =========================================================================

    /**
     * Crea un topico y, opcionalmente, su topico de descartes.
     *
     * Los campos ausentes toman los valores por omision del proyecto. Un topico
     * que ya existia se informa aparte de los creados: declararlo de nuevo no
     * lo reconfigura.
     */
    public ResultadoAdministracion crearTopic(SolicitudTopic solicitud) {
        int particiones = solicitud.particiones() == null
                ? propiedades.getParticiones()
                : solicitud.particiones();

        short replicas = (short) (solicitud.replicas() == null
                ? propiedades.getReplicas()
                : solicitud.replicas());

        long retencion = solicitud.retencionMs() == null
                ? propiedades.getRetencionMs()
                : solicitud.retencionMs();

        // min.insync.replicas por encima del factor de replicacion dejaria al
        // topico sin poder aceptar nunca una escritura acks=all.
        int minimoEnSincro = Math.min(propiedades.getMinInsyncReplicas(), replicas);

        Map<String, String> configuracion = new LinkedHashMap<>();
        configuracion.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(minimoEnSincro));
        configuracion.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retencion));
        configuracion.put(TopicConfig.CLEANUP_POLICY_CONFIG, propiedades.getPoliticaLimpieza());

        List<NewTopic> aCrear = new ArrayList<>();
        aCrear.add(new NewTopic(solicitud.nombre(), particiones, replicas).configs(configuracion));

        if (solicitud.crearDlt()) {
            aCrear.add(new NewTopic(solicitud.nombre() + propiedades.getSufijoDlt(),
                    particiones, replicas).configs(configuracion));
        }

        List<String> creados = new ArrayList<>();
        List<String> yaExistian = new ArrayList<>();

        for (NewTopic topic : aCrear) {
            try {
                admin.createTopics(List.of(topic))
                        .all()
                        .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS);
                creados.add(topic.name());
                log.info("[ADMIN] Topic creado. nombre={} particiones={} replicas={} retencionMs={}",
                        topic.name(), particiones, replicas, retencion);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConsultaKafkaException("Creacion interrumpida para " + topic.name(), e);

            } catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    yaExistian.add(topic.name());
                    log.info("[ADMIN] El topic {} ya existia. No se modifica.", topic.name());
                    continue;
                }
                throw new OperacionKafkaException(
                        "El cluster rechazo la creacion de " + topic.name() + ": "
                                + raiz(e).getMessage(), e);

            } catch (Exception e) {
                throw new ConsultaKafkaException("No fue posible crear el topic " + topic.name(), e);
            }
        }

        List<String> afectados = new ArrayList<>(creados);
        afectados.addAll(yaExistian);

        String detalle = creados.isEmpty()
                ? "No se creo nada nuevo. Ya existian: " + yaExistian
                : "Creados: " + creados + " con " + particiones + " particiones y "
                        + replicas + " replicas."
                        + (yaExistian.isEmpty() ? "" : " Ya existian: " + yaExistian + ".");

        return new ResultadoAdministracion("CREAR", afectados, detalle,
                yaExistian.isEmpty() ? null
                        : "Un topic existente no se reconfigura al declararlo de nuevo. "
                                + "Para cambiarlo use la operacion de configuracion.");
    }

    // =========================================================================
    //  Modificacion
    // =========================================================================

    /**
     * Amplia la cantidad de particiones.
     *
     * Solo admite aumentar: el destino de una clave es hash(clave) % particiones,
     * de modo que reducirlas cambiaria la particion de las claves ya publicadas.
     */
    public ResultadoAdministracion ampliarParticiones(String nombre, SolicitudParticiones solicitud) {
        ResumenTopic actual = describirTopic(nombre);

        if (solicitud.particiones() == actual.particiones()) {
            throw new OperacionKafkaException(
                    "El topic " + nombre + " ya tiene " + actual.particiones() + " particiones");
        }
        if (solicitud.particiones() < actual.particiones()) {
            throw new OperacionKafkaException(
                    "Kafka no permite reducir particiones. El topic " + nombre + " tiene "
                            + actual.particiones() + " y se pidieron " + solicitud.particiones()
                            + ". La particion de destino se calcula a partir del numero de "
                            + "particiones, por lo que quitarlas cambiaria el destino de las "
                            + "claves ya publicadas.");
        }

        try {
            admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(solicitud.particiones())))
                    .all()
                    .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS);

            log.info("[ADMIN] Particiones ampliadas. topic={} de {} a {}",
                    nombre, actual.particiones(), solicitud.particiones());

            return new ResultadoAdministracion("AMPLIAR", List.of(nombre),
                    "El topic " + nombre + " paso de " + actual.particiones() + " a "
                            + solicitud.particiones() + " particiones.",
                    "Los registros ya publicados no se reubican. Las claves nuevas se "
                            + "reparten con el divisor nuevo, de modo que una misma clave puede "
                            + "quedar en dos particiones distintas.");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConsultaKafkaException("Ampliacion interrumpida para " + nombre, e);
        } catch (ExecutionException e) {
            throw new OperacionKafkaException(
                    "El cluster rechazo la ampliacion de " + nombre + ": " + raiz(e).getMessage(), e);
        } catch (Exception e) {
            throw new ConsultaKafkaException("No fue posible ampliar las particiones de " + nombre, e);
        }
    }

    /**
     * Aplica parametros de configuracion a un topico existente.
     *
     * Usa la variante incremental, que modifica solo las claves enviadas. La
     * variante antigua reemplaza la configuracion completa, de modo que toda
     * clave omitida vuelve a su valor heredado del broker.
     */
    public ResultadoAdministracion configurarTopic(String nombre, SolicitudConfiguracion solicitud) {
        // Comprobacion previa para responder 400 en vez de un fallo de AdminClient.
        describirTopic(nombre);

        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);

        Collection<AlterConfigOp> operaciones = new ArrayList<>();
        solicitud.configuracion().forEach((clave, valor) ->
                operaciones.add(new AlterConfigOp(new ConfigEntry(clave, valor),
                        AlterConfigOp.OpType.SET)));

        try {
            admin.incrementalAlterConfigs(Map.of(recurso, operaciones))
                    .all()
                    .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS);

            log.info("[ADMIN] Configuracion modificada. topic={} cambios={}",
                    nombre, solicitud.configuracion());

            return ResultadoAdministracion.de("CONFIGURAR", List.of(nombre),
                    "Se aplicaron " + solicitud.configuracion().size()
                            + " parametros en " + nombre + ": " + solicitud.configuracion());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConsultaKafkaException("Modificacion interrumpida para " + nombre, e);
        } catch (ExecutionException e) {
            throw new OperacionKafkaException(
                    "El cluster rechazo la configuracion de " + nombre + ": "
                            + raiz(e).getMessage()
                            + ". Revise que los nombres de los parametros sean validos.", e);
        } catch (Exception e) {
            throw new ConsultaKafkaException("No fue posible configurar el topic " + nombre, e);
        }
    }

    // =========================================================================
    //  Eliminacion
    // =========================================================================

    /**
     * Elimina un topico.
     *
     * Los topicos internos del cluster quedan fuera del alcance de esta API.
     * Los del negocio exigen confirmar=true.
     *
     * El borrado es asincrono en el broker: la llamada vuelve cuando el cluster
     * acepta la orden, y los segmentos se descartan despues.
     */
    public ResultadoAdministracion eliminarTopic(String nombre, boolean confirmar) {
        if (nombre.startsWith(PREFIJO_INTERNO)) {
            throw new OperacionKafkaException(
                    "El topic " + nombre + " es interno del cluster y no se puede eliminar "
                            + "desde esta API. Ahi el cluster guarda informacion de la que "
                            + "dependen todos los grupos consumidores.");
        }

        boolean esDelNegocio = propiedades.getTopicsDelNegocio().stream()
                .anyMatch(t -> t.equals(nombre) || (t + propiedades.getSufijoDlt()).equals(nombre));

        if (esDelNegocio && !confirmar) {
            throw new OperacionKafkaException(
                    "El topic " + nombre + " pertenece al flujo de pedidos. Para eliminarlo "
                            + "debe enviar confirmar=true. La eliminacion borra el log completo "
                            + "y no se puede deshacer.");
        }

        try {
            admin.deleteTopics(List.of(nombre))
                    .all()
                    .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS);

            log.warn("[ADMIN] Topic eliminado. nombre={} eraDelNegocio={}", nombre, esDelNegocio);

            return new ResultadoAdministracion("ELIMINAR", List.of(nombre),
                    "Se envio la orden de eliminar " + nombre + ".",
                    "El borrado es asincrono: el topic puede seguir apareciendo unos segundos "
                            + "mientras el broker descarta los segmentos. Si el topic esta "
                            + "declarado en la topologia, volvera a crearse al reiniciar "
                            + "ms-kafka.");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConsultaKafkaException("Eliminacion interrumpida para " + nombre, e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                throw new OperacionKafkaException("El topic " + nombre + " no existe en el cluster");
            }
            throw new OperacionKafkaException(
                    "El cluster rechazo la eliminacion de " + nombre + ": " + raiz(e).getMessage(), e);
        } catch (Exception e) {
            throw new ConsultaKafkaException("No fue posible eliminar el topic " + nombre, e);
        }
    }

    // =========================================================================
    //  Grupos consumidores
    // =========================================================================

    /**
     * Grupos consumidores con su estado, sus miembros y su retraso total.
     *
     * Un retraso que crece de forma sostenida indica que el consumo no alcanza
     * el ritmo de la publicacion. Subir la concurrencia solo ayuda mientras
     * haya particiones libres: un grupo no puede tener mas consumidores activos
     * que particiones.
     */
    public List<ResumenGrupo> listarGrupos() {
        try {
            Collection<ConsumerGroupListing> listado = admin.listConsumerGroups()
                    .all()
                    .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS);

            List<String> ids = listado.stream().map(ConsumerGroupListing::groupId).sorted().toList();
            if (ids.isEmpty()) {
                return List.of();
            }

            Map<String, ConsumerGroupDescription> descripciones = admin.describeConsumerGroups(ids)
                    .all()
                    .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS);

            List<ResumenGrupo> resumenes = new ArrayList<>();
            for (String id : ids) {
                ConsumerGroupDescription descripcion = descripciones.get(id);

                Map<TopicPartition, OffsetAndMetadata> confirmados = admin
                        .listConsumerGroupOffsets(id)
                        .partitionsToOffsetAndMetadata()
                        .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS);

                long retraso = calcularRetrasoTotal(confirmados);

                Set<String> topics = new TreeSet<>();
                confirmados.keySet().forEach(tp -> topics.add(tp.topic()));

                resumenes.add(new ResumenGrupo(
                        id,
                        descripcion == null ? "DESCONOCIDO" : descripcion.state().toString(),
                        descripcion == null ? 0 : descripcion.members().size(),
                        List.copyOf(topics),
                        retraso));
            }
            return resumenes;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConsultaKafkaException("Consulta interrumpida al listar los grupos", e);
        } catch (Exception e) {
            throw new ConsultaKafkaException(
                    "No fue posible listar los grupos consumidores. Verifique el cluster.", e);
        }
    }

    // =========================================================================
    //  Auxiliares
    // =========================================================================

    /**
     * Suma, sobre las particiones confirmadas por el grupo, la diferencia entre
     * el final del log y el offset confirmado.
     */
    private long calcularRetrasoTotal(Map<TopicPartition, OffsetAndMetadata> confirmados)
            throws Exception {

        if (confirmados.isEmpty()) {
            return 0L;
        }

        Map<TopicPartition, OffsetSpec> consulta = new HashMap<>();
        confirmados.keySet().forEach(tp -> consulta.put(tp, OffsetSpec.latest()));

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> finales = admin
                .listOffsets(consulta)
                .all()
                .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS);

        long total = 0L;
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entrada : confirmados.entrySet()) {
            var fin = finales.get(entrada.getKey());
            if (fin == null || entrada.getValue() == null) {
                continue;
            }
            total += Math.max(0L, fin.offset() - entrada.getValue().offset());
        }
        return total;
    }

    /**
     * Configuracion de varios topicos en una sola llamada.
     *
     * Conserva unicamente los valores fijados en el topico; los heredados del
     * broker son decenas y no describen la topologia de la aplicacion. Un fallo
     * aqui devuelve el mapa vacio en lugar de abortar la consulta completa.
     */
    private Map<String, Map<String, String>> leerConfiguraciones(Set<String> nombres) {
        List<ConfigResource> recursos = nombres.stream()
                .map(n -> new ConfigResource(ConfigResource.Type.TOPIC, n))
                .toList();

        try {
            Map<ConfigResource, Config> respuesta = admin.describeConfigs(recursos)
                    .all()
                    .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS);

            Map<String, Map<String, String>> porTopic = new LinkedHashMap<>();
            respuesta.forEach((recurso, config) -> {
                Map<String, String> valores = new LinkedHashMap<>();
                for (ConfigEntry entrada : config.entries()) {
                    if (entrada.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                        valores.put(entrada.name(), entrada.value());
                    }
                }
                porTopic.put(recurso.name(), valores);
            });
            return porTopic;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConsultaKafkaException("Consulta de configuracion interrumpida", e);
        } catch (Exception e) {
            log.warn("[ADMIN] No se pudo leer la configuracion de los topics: {}", e.getMessage());
            return Map.of();
        }
    }

    /** Convierte la respuesta de AdminClient en el DTO de salida. */
    private ResumenTopic armarResumen(TopicDescription descripcion, Map<String, String> configuracion) {
        List<ResumenTopic.ParticionResumen> detalle = new ArrayList<>();

        for (TopicPartitionInfo info : descripcion.partitions()) {
            Node lider = info.leader();
            detalle.add(new ResumenTopic.ParticionResumen(
                    info.partition(),
                    lider == null ? -1 : lider.id(),
                    info.replicas().stream().map(Node::id).toList(),
                    info.isr().stream().map(Node::id).toList()));
        }
        detalle.sort(Comparator.comparingInt(ResumenTopic.ParticionResumen::particion));

        int replicas = descripcion.partitions().isEmpty()
                ? 0
                : descripcion.partitions().get(0).replicas().size();

        return new ResumenTopic(
                descripcion.name(),
                descripcion.partitions().size(),
                replicas,
                descripcion.isInternal() || descripcion.name().startsWith(PREFIJO_INTERNO),
                descripcion.name().endsWith(propiedades.getSufijoDlt()),
                detalle,
                configuracion);
    }

    private Throwable raiz(ExecutionException e) {
        return e.getCause() == null ? e : e.getCause();
    }
}
