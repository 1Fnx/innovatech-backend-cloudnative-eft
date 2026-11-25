package cl.innovatech.pedidos.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Enlaza el bloque "app.rabbit" del archivo application.yml con objetos Java.
 *
 * Mantener los nombres en un unico punto evita que queden repetidos como
 * literales a lo largo del codigo. Renombrar una cola o un exchange se resuelve
 * editando el application.yml, sin tocar las clases que los consumen.
 */
@Component
@ConfigurationProperties(prefix = "app.rabbit")
public class MensajeriaProperties {

    private Exchanges exchanges = new Exchanges();
    private RoutingKeys routingKeys = new RoutingKeys();
    private Queues queues = new Queues();
    private Reintentos reintentos = new Reintentos();

    /** Exchanges declarados por el sistema. */
    public static class Exchanges {
        /** Exchange de negocio donde se publican los eventos de pedidos. */
        private String pedidos;
        /** Exchange de avisos operativos, con coincidencia exacta de clave. */
        private String alertas;
        /** Dead Letter Exchange: recibe los mensajes que no se pudieron procesar. */
        private String dlx;

        public String getPedidos() { return pedidos; }
        public void setPedidos(String pedidos) { this.pedidos = pedidos; }
        public String getAlertas() { return alertas; }
        public void setAlertas(String alertas) { this.alertas = alertas; }
        public String getDlx() { return dlx; }
        public void setDlx(String dlx) { this.dlx = dlx; }
    }

    /** Claves de enrutamiento usadas por los bindings. */
    public static class RoutingKeys {
        private String pedidoCreado;
        private String alertaStockBajo;
        private String dlqNotificaciones;
        private String dlqDocumentos;
        private String dlqInventario;
        private String dlqAlertas;

        public String getPedidoCreado() { return pedidoCreado; }
        public void setPedidoCreado(String v) { this.pedidoCreado = v; }
        public String getAlertaStockBajo() { return alertaStockBajo; }
        public void setAlertaStockBajo(String v) { this.alertaStockBajo = v; }
        public String getDlqAlertas() { return dlqAlertas; }
        public void setDlqAlertas(String v) { this.dlqAlertas = v; }
        public String getDlqNotificaciones() { return dlqNotificaciones; }
        public void setDlqNotificaciones(String v) { this.dlqNotificaciones = v; }
        public String getDlqDocumentos() { return dlqDocumentos; }
        public void setDlqDocumentos(String v) { this.dlqDocumentos = v; }
        public String getDlqInventario() { return dlqInventario; }
        public void setDlqInventario(String v) { this.dlqInventario = v; }
    }

    /** Colas de trabajo y sus respectivas Dead Letter Queues. */
    public static class Queues {
        private String notificaciones;
        private String documentos;
        private String inventario;
        private String alertasStock;
        private String notificacionesDlq;
        private String documentosDlq;
        private String inventarioDlq;
        private String alertasStockDlq;

        public String getNotificaciones() { return notificaciones; }
        public void setNotificaciones(String v) { this.notificaciones = v; }
        public String getDocumentos() { return documentos; }
        public void setDocumentos(String v) { this.documentos = v; }
        public String getInventario() { return inventario; }
        public void setInventario(String v) { this.inventario = v; }
        public String getNotificacionesDlq() { return notificacionesDlq; }
        public void setNotificacionesDlq(String v) { this.notificacionesDlq = v; }
        public String getDocumentosDlq() { return documentosDlq; }
        public void setDocumentosDlq(String v) { this.documentosDlq = v; }
        public String getInventarioDlq() { return inventarioDlq; }
        public void setInventarioDlq(String v) { this.inventarioDlq = v; }
        public String getAlertasStock() { return alertasStock; }
        public void setAlertasStock(String v) { this.alertasStock = v; }
        public String getAlertasStockDlq() { return alertasStockDlq; }
        public void setAlertasStockDlq(String v) { this.alertasStockDlq = v; }
    }

    /** Politica de reintentos aplicada por los consumidores. */
    public static class Reintentos {
        private int maxIntentos = 2;
        public int getMaxIntentos() { return maxIntentos; }
        public void setMaxIntentos(int v) { this.maxIntentos = v; }
    }

    public Exchanges getExchanges() { return exchanges; }
    public void setExchanges(Exchanges e) { this.exchanges = e; }
    public RoutingKeys getRoutingKeys() { return routingKeys; }
    public void setRoutingKeys(RoutingKeys r) { this.routingKeys = r; }
    public Queues getQueues() { return queues; }
    public void setQueues(Queues q) { this.queues = q; }
    public Reintentos getReintentos() { return reintentos; }
    public void setReintentos(Reintentos r) { this.reintentos = r; }
}
