package cl.innovatech.pedidos.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Enlace tipado del bloque {@code app.kafka} de application.yml.
 *
 * Contraparte de MensajeriaProperties para el log distribuido: los nombres de
 * topico no se escriben como literal en ninguna clase.
 */
@Component
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaProperties {

    private Topics topics = new Topics();
    private int particiones = 3;
    private int replicas = 3;

    /**
     * Los dos topicos del sistema. Van separados porque transportan hechos
     * distintos, los consumen grupos distintos y admiten politicas de retencion
     * distintas.
     */
    public static class Topics {

        /** Pedidos creados. Lo leen el grupo de inventario y el de notificaciones. */
        private String pedidosEventos;

        /** Movimientos de stock publicados por ms-productos tras el descuento. */
        private String inventarioMovimientos;

        public String getPedidosEventos() { return pedidosEventos; }
        public void setPedidosEventos(String v) { this.pedidosEventos = v; }
        public String getInventarioMovimientos() { return inventarioMovimientos; }
        public void setInventarioMovimientos(String v) { this.inventarioMovimientos = v; }
    }

    public Topics getTopics() { return topics; }
    public void setTopics(Topics t) { this.topics = t; }
    public int getParticiones() { return particiones; }
    public void setParticiones(int v) { this.particiones = v; }
    public int getReplicas() { return replicas; }
    public void setReplicas(int v) { this.replicas = v; }
}
