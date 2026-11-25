package cl.innovatech.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Enlace tipado del bloque {@code app.kafka} de application.yml.
 *
 * Describe la topologia del sistema y los valores por omision que se aplican al
 * crear un topico desde la API de administracion.
 */
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaProperties {

    /** Topico de pruebas del propio microservicio. */
    private String topic;

    private int particiones = 3;

    /** Copias de cada particion. No puede superar el numero de brokers. */
    private int replicas = 3;

    /**
     * Copias que deben confirmar una escritura acks=all para darla por aceptada.
     * Con replicas=3 y este valor en 2 se tolera la caida de un broker sin
     * dejar de aceptar escrituras.
     */
    private int minInsyncReplicas = 2;

    private int retencionDias = 7;

    /** delete descarta por antiguedad; compact conserva el ultimo valor por clave. */
    private String politicaLimpieza = "delete";

    private String grupoId;

    /** Tope de la ventana de registros consumidos que se guarda en memoria. */
    private int bitacoraMaxima = 200;

    private int esperaConfirmacionSegundos = 15;

    /** Sufijo que Spring Kafka agrega al topico de origen para los descartes. */
    private String sufijoDlt = ".DLT";

    /**
     * Topicos que este microservicio declara al arrancar. Se listan aqui para
     * que exista un unico responsable de la topologia: los microservicios que
     * publican y consumen no crean infraestructura.
     */
    private List<String> topicsDelNegocio = new ArrayList<>();

    /** Grupos consumidores del sistema, para la vista de operacion. */
    private List<String> gruposDelNegocio = new ArrayList<>();

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public int getParticiones() { return particiones; }
    public void setParticiones(int particiones) { this.particiones = particiones; }

    public int getReplicas() { return replicas; }
    public void setReplicas(int replicas) { this.replicas = replicas; }

    public int getMinInsyncReplicas() { return minInsyncReplicas; }
    public void setMinInsyncReplicas(int v) { this.minInsyncReplicas = v; }

    public int getRetencionDias() { return retencionDias; }
    public void setRetencionDias(int retencionDias) { this.retencionDias = retencionDias; }

    public String getPoliticaLimpieza() { return politicaLimpieza; }
    public void setPoliticaLimpieza(String politicaLimpieza) { this.politicaLimpieza = politicaLimpieza; }

    public String getGrupoId() { return grupoId; }
    public void setGrupoId(String grupoId) { this.grupoId = grupoId; }

    public int getBitacoraMaxima() { return bitacoraMaxima; }
    public void setBitacoraMaxima(int bitacoraMaxima) { this.bitacoraMaxima = bitacoraMaxima; }

    public int getEsperaConfirmacionSegundos() { return esperaConfirmacionSegundos; }
    public void setEsperaConfirmacionSegundos(int v) { this.esperaConfirmacionSegundos = v; }

    public String getSufijoDlt() { return sufijoDlt; }
    public void setSufijoDlt(String sufijoDlt) { this.sufijoDlt = sufijoDlt; }

    public List<String> getTopicsDelNegocio() { return topicsDelNegocio; }
    public void setTopicsDelNegocio(List<String> t) { this.topicsDelNegocio = t; }

    public List<String> getGruposDelNegocio() { return gruposDelNegocio; }
    public void setGruposDelNegocio(List<String> g) { this.gruposDelNegocio = g; }

    /** Retencion en milisegundos, que es la unidad que espera Kafka. */
    public long getRetencionMs() {
        return retencionDias * 24L * 60L * 60L * 1000L;
    }
}
