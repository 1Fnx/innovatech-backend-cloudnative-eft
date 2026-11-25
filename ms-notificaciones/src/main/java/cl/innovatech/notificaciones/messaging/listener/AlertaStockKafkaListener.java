package cl.innovatech.notificaciones.messaging.listener;

import cl.innovatech.notificaciones.messaging.evento.MovimientoInventarioEvento;
import cl.innovatech.notificaciones.service.RegistroProcesoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

/**
 * Consumo del topico de movimientos: detecta stock bajo y deja la alerta
 * registrada. Cierra el recorrido pedido -> inventario -> alerta.
 *
 * Nadie invoca a este microservicio para que revise el stock: reacciona porque
 * esta suscrito. Un interesado nuevo en los mismos movimientos se suma con un
 * grupo propio, sin tocar a ms-productos.
 *
 * Grupo distinto del listener de pedidos, de modo que los dos flujos avanzan
 * por separado y uno no retrasa al otro.
 */
@Component
public class AlertaStockKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(AlertaStockKafkaListener.class);
    private static final String DOMINIO = "ALERTAS KAFKA";

    private final RegistroProcesoService bitacora;

    public AlertaStockKafkaListener(RegistroProcesoService bitacora) {
        this.bitacora = bitacora;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventario-movimientos}",
            groupId = "${app.kafka.grupos.alertas}",
            containerFactory = "fabricaMovimientos")
    public void recibirMovimiento(@Payload MovimientoInventarioEvento evento,
                                  @Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
                                  @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("[{}] Movimiento recibido. particion={} offset={} pedidoId={} lineas={} stockBajo={}",
                DOMINIO, particion, offset, evento.pedidoId(),
                evento.lineas() == null ? 0 : evento.lineas().size(), evento.hayStockBajo());

        if (!evento.hayStockBajo()) {
            log.info("[{}] Sin alertas que emitir. pedidoId={}", DOMINIO, evento.pedidoId());
            return;
        }

        String detalle = evento.lineas().stream()
                .filter(l -> l.stockResultante() < 10)
                .map(l -> l.nombre() + " quedo en " + l.stockResultante())
                .collect(Collectors.joining("; "));

        log.warn("[{}] ALERTA DE STOCK BAJO. pedidoId={} -> {}", DOMINIO, evento.pedidoId(), detalle);

        bitacora.registrar(DOMINIO, evento.pedidoId(), null, "ALERTA", detalle);
    }
}
