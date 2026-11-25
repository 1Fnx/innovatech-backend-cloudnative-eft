package cl.innovatech.productos.messaging.producer;

import cl.innovatech.productos.config.MensajeriaProperties;
import cl.innovatech.productos.messaging.evento.AlertaStockEvento;
import cl.innovatech.productos.messaging.evento.MovimientoInventarioEvento;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

/**
 * Publicacion de la alerta de reposicion por el canal de colas.
 *
 * El exchange es de tipo direct: la clave de enrutamiento se compara de forma
 * exacta contra la del binding, sin patrones. Es el tipo adecuado cuando el
 * aviso tiene un destinatario definido, a diferencia del exchange de pedidos,
 * donde un mismo evento se copia a tres colas distintas segun el patron.
 *
 * El aviso se emite unicamente cuando hay lineas bajo el umbral; si no las hay,
 * no se publica nada.
 */
@Component
public class AlertaStockPublisher {

    private static final Logger log = LoggerFactory.getLogger(AlertaStockPublisher.class);
    private static final DateTimeFormatter FORMATO = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final RabbitTemplate plantilla;
    private final MensajeriaProperties propiedades;

    @Value("${app.inventario.umbral-stock-bajo:10}")
    private int umbralStockBajo;

    public AlertaStockPublisher(RabbitTemplate rabbitTemplate, MensajeriaProperties propiedades) {
        this.plantilla = rabbitTemplate;
        this.propiedades = propiedades;
    }

    public void publicarAlerta(Long pedidoId, List<MovimientoInventarioEvento.Linea> lineas) {

        List<AlertaStockEvento.Linea> bajoUmbral = lineas.stream()
                .filter(l -> l.stockResultante() < umbralStockBajo)
                .map(l -> new AlertaStockEvento.Linea(
                        l.productoId(), l.nombre(), l.stockResultante()))
                .toList();

        if (bajoUmbral.isEmpty()) {
            return;
        }

        AlertaStockEvento alerta = new AlertaStockEvento(
                UUID.randomUUID().toString(),
                pedidoId,
                umbralStockBajo,
                bajoUmbral,
                LocalDateTime.now().format(FORMATO));

        String exchange = propiedades.getExchanges().getAlertas();
        String clave = propiedades.getRoutingKeys().getAlertaStockBajo();

        try {
            plantilla.convertAndSend(exchange, clave, alerta);
            log.warn("[ALERTA STOCK] Publicada. exchange={} routingKey={} pedidoId={} lineas={}",
                    exchange, clave, pedidoId, bajoUmbral.size());
        } catch (Exception e) {
            log.error("[ALERTA STOCK] No se pudo publicar. pedidoId={} causa={}",
                    pedidoId, e.getMessage(), e);
        }
    }
}
