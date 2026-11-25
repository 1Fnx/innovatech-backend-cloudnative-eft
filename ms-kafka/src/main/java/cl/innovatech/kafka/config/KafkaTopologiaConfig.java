package cl.innovatech.kafka.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Declaracion programatica de la topologia.
 *
 * El cluster corre con auto.create.topics.enable en false, de modo que los
 * topicos deben existir antes de la primera publicacion. KafkaAdmin compara lo
 * declarado aqui contra el cluster al arrancar y crea lo que falte; la
 * operacion es idempotente.
 *
 * Lo que ya existe no se reconfigura. En particular, Kafka no admite reducir
 * particiones: el destino de una clave es hash(clave) % particiones, asi que
 * quitarlas cambiaria la particion de las claves ya publicadas. Para ampliar
 * esta el endpoint correspondiente en AdministracionTopicsService.
 */
@Configuration
public class KafkaTopologiaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopologiaConfig.class);

    private final KafkaProperties propiedades;

    public KafkaTopologiaConfig(KafkaProperties propiedades) {
        this.propiedades = propiedades;
    }

    /**
     * Topicos del negocio y su correspondiente topico de descartes.
     *
     * Se agrupan en KafkaAdmin.NewTopics porque KafkaAdmin solo reconoce beans
     * de tipo NewTopic y NewTopics; un List&lt;NewTopic&gt; no se procesa.
     */
    @Bean
    public KafkaAdmin.NewTopics topicsDelNegocio() {
        List<NewTopic> declarados = new ArrayList<>();

        for (String nombre : propiedades.getTopicsDelNegocio()) {
            declarados.add(construir(nombre, propiedades.getParticiones()));
            declarados.add(construir(nombre + propiedades.getSufijoDlt(), propiedades.getParticiones()));
        }

        log.info("[TOPOLOGIA] Se declaran {} topics (negocio + DLT): {}",
                declarados.size(), declarados.stream().map(NewTopic::name).toList());

        return new KafkaAdmin.NewTopics(declarados.toArray(new NewTopic[0]));
    }

    /** Topico de pruebas; se puede vaciar o recrear sin afectar al negocio. */
    @Bean
    public NewTopic topicDelLaboratorio() {
        return construir(propiedades.getTopic(), propiedades.getParticiones());
    }

    /**
     * Configuracion comun de todo topico creado por la aplicacion.
     *
     *   min.insync.replicas  copias que deben confirmar una escritura acks=all
     *   retention.ms         plazo tras el cual el broker descarta el registro;
     *                        en Kafka el mensaje no se borra al ser leido
     *   cleanup.policy       delete descarta por antiguedad, compact conserva
     *                        solo el ultimo valor de cada clave
     */
    private NewTopic construir(String nombre, int particiones) {
        Map<String, String> configuracion = new LinkedHashMap<>();
        configuracion.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                String.valueOf(propiedades.getMinInsyncReplicas()));
        configuracion.put(TopicConfig.RETENTION_MS_CONFIG,
                String.valueOf(propiedades.getRetencionMs()));
        configuracion.put(TopicConfig.CLEANUP_POLICY_CONFIG,
                propiedades.getPoliticaLimpieza());

        return new NewTopic(nombre, particiones, (short) propiedades.getReplicas())
                .configs(configuracion);
    }

    /**
     * Cliente de administracion del cluster.
     *
     * Reutiliza la configuracion que Spring Boot preparo para KafkaAdmin, de
     * modo que la direccion de los brokers no se repite.
     */
    @Bean(destroyMethod = "close")
    public AdminClient adminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }
}
