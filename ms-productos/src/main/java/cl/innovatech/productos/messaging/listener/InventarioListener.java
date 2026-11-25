package cl.innovatech.productos.messaging.listener;

import cl.innovatech.productos.exception.ErrorNoRecuperableException;
import cl.innovatech.productos.exception.ErrorRecuperableException;
import cl.innovatech.productos.messaging.evento.MovimientoInventarioEvento;
import cl.innovatech.productos.messaging.evento.PedidoEvento;
import cl.innovatech.productos.messaging.producer.AlertaStockPublisher;
import cl.innovatech.productos.service.InventarioService;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

/**
 * Consumidor del dominio de inventario, alojado en el servicio responsable
 * del catalogo porque es el dueno del stock.
 *
 * Mantiene el mismo contrato de confirmacion que el resto de los consumidores:
 *   exito -> ACK | transitorio -> un reintento | definitivo -> DLQ
 *
 * Tras descontar el stock evalua el umbral de reposicion. Si alguna linea
 * quedo por debajo, emite la alerta por el exchange direct antes de confirmar.
 */
@Component
public class InventarioListener {

    private static final Logger log = LoggerFactory.getLogger(InventarioListener.class);
    private static final String DOMINIO = "INVENTARIO";

    private final InventarioService servicio;
    private final AlertaStockPublisher alertas;

    public InventarioListener(InventarioService servicio, AlertaStockPublisher alertas) {
        this.servicio = servicio;
        this.alertas = alertas;
    }

    @RabbitListener(queues = "${app.rabbit.queues.inventario}")
    public void recibirPedidoCreado(@Payload PedidoEvento evento,
                                    @Header(AmqpHeaders.DELIVERY_TAG) long etiquetaEntrega,
                                    @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean reentregado,
                                    Channel canal) throws IOException {

        boolean esReintento = Boolean.TRUE.equals(reentregado);

        log.info("[{}] Mensaje recibido. pedidoId={} items={} reintento={}",
                DOMINIO, evento.pedidoId(),
                evento.items() == null ? 0 : evento.items().size(), esReintento);

        try {
            List<MovimientoInventarioEvento.Linea> lineas = servicio.descontarStock(evento);

            if (servicio.hayStockBajo(lineas)) {
                alertas.publicarAlerta(evento.pedidoId(), lineas);
            }

            canal.basicAck(etiquetaEntrega, false);
            log.info("[{}] ACK enviado. Inventario actualizado. pedidoId={}", DOMINIO, evento.pedidoId());

        } catch (ErrorNoRecuperableException e) {
            log.error("[{}] Error NO recuperable. NACK sin reencolar -> DLQ. pedidoId={} causa={}",
                    DOMINIO, evento.pedidoId(), e.getMessage());
            canal.basicNack(etiquetaEntrega, false, false);

        } catch (ErrorRecuperableException e) {
            if (!esReintento) {
                log.warn("[{}] Error recuperable, primer intento. NACK con reencolado. pedidoId={} causa={}",
                        DOMINIO, evento.pedidoId(), e.getMessage());
                canal.basicNack(etiquetaEntrega, false, true);
            } else {
                log.error("[{}] Error recuperable persistente. NACK sin reencolar -> DLQ. pedidoId={} causa={}",
                        DOMINIO, evento.pedidoId(), e.getMessage());
                canal.basicNack(etiquetaEntrega, false, false);
            }

        } catch (Exception e) {
            log.error("[{}] Error inesperado. NACK sin reencolar -> DLQ. pedidoId={}",
                    DOMINIO, evento.pedidoId(), e);
            canal.basicNack(etiquetaEntrega, false, false);
        }
    }
}
