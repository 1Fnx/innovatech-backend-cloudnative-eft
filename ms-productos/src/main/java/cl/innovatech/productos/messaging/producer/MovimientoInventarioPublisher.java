package cl.innovatech.productos.messaging.producer;

import cl.innovatech.productos.messaging.evento.MovimientoInventarioEvento;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

/**
 * Publicacion del movimiento de inventario, segunda etapa del pipeline.
 *
 * La clave vuelve a ser el identificador del pedido, de modo que el movimiento
 * cae en la misma particion que el evento que lo origino y los dos topicos
 * conservan el mismo criterio de orden.
 */
@Component
public class MovimientoInventarioPublisher {

    private static final Logger log = LoggerFactory.getLogger(MovimientoInventarioPublisher.class);
    private static final DateTimeFormatter FORMATO = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final KafkaTemplate<String, MovimientoInventarioEvento> kafkaTemplate;

    @Value("${app.kafka.topics.inventario-movimientos}")
    private String topicMovimientos;

    public MovimientoInventarioPublisher(
            KafkaTemplate<String, MovimientoInventarioEvento> kafkaTemplateMovimientos) {
        this.kafkaTemplate = kafkaTemplateMovimientos;
    }

    public void publicarMovimiento(Long pedidoId,
                                   List<MovimientoInventarioEvento.Linea> lineas,
                                   boolean hayStockBajo) {

        MovimientoInventarioEvento evento = new MovimientoInventarioEvento(
                UUID.randomUUID().toString(),
                MovimientoInventarioEvento.TIPO_STOCK_DESCONTADO,
                pedidoId,
                lineas,
                hayStockBajo,
                LocalDateTime.now().format(FORMATO));

        String clave = String.valueOf(pedidoId);

        kafkaTemplate.send(topicMovimientos, clave, evento)
                .whenComplete((resultado, error) -> {
                    if (error != null) {
                        log.error("[MOVIMIENTOS] No se pudo publicar. pedidoId={} causa={}",
                                pedidoId, error.getMessage(), error);
                        return;
                    }
                    log.info("[MOVIMIENTOS] Publicado. pedidoId={} particion={} offset={} stockBajo={}",
                            pedidoId,
                            resultado.getRecordMetadata().partition(),
                            resultado.getRecordMetadata().offset(),
                            hayStockBajo);
                });
    }
}
