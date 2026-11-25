package cl.innovatech.kafka.service;

import cl.innovatech.kafka.config.KafkaProperties;
import cl.innovatech.kafka.dto.DescripcionTopic;
import cl.innovatech.kafka.dto.EstadoGrupo;
import cl.innovatech.kafka.exception.ConsultaKafkaException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Consulta metadatos del cluster usando el cliente de administracion oficial.
 *
 * Expone por HTTP las mismas cifras que entregan dos comandos de la guia:
 *   kafka-topics.sh --describe            -> particiones, lider y replicas
 *   kafka-consumer-groups.sh --describe   -> CURRENT-OFFSET, LOG-END-OFFSET, LAG
 *
 * El objetivo es que esos valores se puedan observar tambien desde la pantalla
 * del frontend, sin dejar de poder comprobarlos por linea de comandos. Las
 * cifras provienen del broker en ambos casos, de modo que deben coincidir.
 */
@Service
public class ObservabilidadKafkaService {

    /** Tiempo limite de cada consulta al broker, en segundos. */
    private static final int LIMITE_SEGUNDOS = 10;

    private final AdminClient admin;
    private final KafkaProperties propiedades;

    public ObservabilidadKafkaService(AdminClient admin, KafkaProperties propiedades) {
        this.admin = admin;
        this.propiedades = propiedades;
    }

    /**
     * Describe el topic configurado: cuantas particiones tiene, cuantas
     * replicas y en que posicion va el log de cada particion.
     *
     * El ultimo offset de cada particion es la posicion que se asignara al
     * siguiente registro. Por eso, en una particion con tres registros escritos
     * el valor informado es 3 y no 2.
     */
    public DescripcionTopic describirTopic() {
        return describirTopic(propiedades.getTopic());
    }

    /** Misma consulta, sobre cualquier topico del cluster. */
    public DescripcionTopic describirTopic(String topic) {
        try {
            TopicDescription descripcion = admin
                    .describeTopics(List.of(topic))
                    .allTopicNames()
                    .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS)
                    .get(topic);

            // Se pregunta por el final del log de cada particion. Es el mismo
            // valor que la columna LOG-END-OFFSET del comando de grupos.
            Map<TopicPartition, OffsetSpec> consulta = new HashMap<>();
            for (TopicPartitionInfo info : descripcion.partitions()) {
                consulta.put(new TopicPartition(topic, info.partition()), OffsetSpec.latest());
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> finales = admin
                    .listOffsets(consulta)
                    .all()
                    .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS);

            List<DescripcionTopic.ParticionDetalle> detalle = new ArrayList<>();
            for (TopicPartitionInfo info : descripcion.partitions()) {
                TopicPartition tp = new TopicPartition(topic, info.partition());
                long ultimo = finales.containsKey(tp) ? finales.get(tp).offset() : 0L;
                Node lider = info.leader();
                detalle.add(new DescripcionTopic.ParticionDetalle(
                        info.partition(),
                        lider == null ? -1 : lider.id(),
                        info.replicas().stream().map(Node::id).toList(),
                        ultimo));
            }
            detalle.sort(Comparator.comparingInt(DescripcionTopic.ParticionDetalle::particion));

            int replicas = descripcion.partitions().isEmpty()
                    ? propiedades.getReplicas()
                    : descripcion.partitions().get(0).replicas().size();

            return new DescripcionTopic(topic, descripcion.partitions().size(), replicas, detalle);

        } catch (InterruptedException e) {
            // Se restituye la marca de interrupcion para no ocultar la senal.
            Thread.currentThread().interrupt();
            throw new ConsultaKafkaException("Consulta interrumpida al describir el topic " + topic, e);
        } catch (Exception e) {
            throw new ConsultaKafkaException(
                    "No fue posible describir el topic " + topic
                            + ". Verifique que el broker este en ejecucion y que el topic exista.", e);
        }
    }

    /**
     * Describe el avance del grupo consumidor particion por particion.
     *
     * Tres cifras y su lectura:
     *   offsetActual   : ultima posicion confirmada por el grupo. Es el punto
     *                    desde el que retomaria si se reinicia la aplicacion.
     *   offsetFinalLog : hasta donde llego el log en esa particion.
     *   retraso        : cuanto le falta leer al grupo. Un retraso en cero con
     *                    el consumidor activo indica que esta al dia.
     *
     * Si el grupo todavia no confirma offsets, el valor informado es -1: no
     * existe posicion confirmada, que es distinto de haber confirmado el cero.
     * Con confirmacion automatica activada ese estado dura poco, por lo que si
     * se consulta de inmediato tras publicar conviene esperar unos segundos y
     * repetir.
     */
    public EstadoGrupo describirGrupo() {
        return describirGrupo(propiedades.getGrupoId(), propiedades.getTopic());
    }

    /** Mismo calculo, para cualquier combinacion de grupo y topico. */
    public EstadoGrupo describirGrupo(String grupo, String topic) {
        try {
            ConsumerGroupDescription descripcionGrupo = admin
                    .describeConsumerGroups(List.of(grupo))
                    .all()
                    .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS)
                    .get(grupo);

            Map<TopicPartition, OffsetAndMetadata> confirmados = admin
                    .listConsumerGroupOffsets(grupo)
                    .partitionsToOffsetAndMetadata()
                    .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS);

            // Se consulta el final del log de todas las particiones del topic,
            // no solo de las que el grupo ya confirmo, para que el listado
            // quede completo desde la primera consulta.
            TopicDescription descripcionTopic = admin
                    .describeTopics(List.of(topic))
                    .allTopicNames()
                    .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS)
                    .get(topic);

            Map<TopicPartition, OffsetSpec> consulta = new HashMap<>();
            for (TopicPartitionInfo info : descripcionTopic.partitions()) {
                consulta.put(new TopicPartition(topic, info.partition()), OffsetSpec.latest());
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> finales = admin
                    .listOffsets(consulta)
                    .all()
                    .get(LIMITE_SEGUNDOS, TimeUnit.SECONDS);

            List<EstadoGrupo.AvanceParticion> avance = new ArrayList<>();
            for (TopicPartitionInfo info : descripcionTopic.partitions()) {
                TopicPartition tp = new TopicPartition(topic, info.partition());
                long fin = finales.containsKey(tp) ? finales.get(tp).offset() : 0L;
                OffsetAndMetadata confirmado = confirmados.get(tp);
                long actual = confirmado == null ? -1L : confirmado.offset();
                long retraso = confirmado == null ? fin : Math.max(0L, fin - actual);
                avance.add(new EstadoGrupo.AvanceParticion(info.partition(), actual, fin, retraso));
            }
            avance.sort(Comparator.comparingInt(EstadoGrupo.AvanceParticion::particion));

            return new EstadoGrupo(
                    grupo,
                    descripcionGrupo == null ? "DESCONOCIDO" : descripcionGrupo.state().toString(),
                    descripcionGrupo == null ? 0 : descripcionGrupo.members().size(),
                    avance);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConsultaKafkaException("Consulta interrumpida al describir el grupo " + grupo, e);
        } catch (Exception e) {
            throw new ConsultaKafkaException(
                    "No fue posible describir el grupo consumidor " + grupo
                            + ". Verifique que el broker este en ejecucion.", e);
        }
    }
}
