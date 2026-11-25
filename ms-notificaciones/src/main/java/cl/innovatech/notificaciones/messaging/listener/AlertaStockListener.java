package cl.innovatech.notificaciones.messaging.listener;

import cl.innovatech.notificaciones.exception.ErrorNoRecuperableException;
import cl.innovatech.notificaciones.exception.ErrorRecuperableException;
import cl.innovatech.notificaciones.messaging.evento.AlertaStockEvento;
import cl.innovatech.notificaciones.service.RegistroProcesoService;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.stream.Collectors;

/**
 * Consumidor de la cola de reposicion.
 *
 * Es el unico suscriptor del exchange direct: la alerta tiene un destinatario
 * definido y no se reparte entre varias colas, a diferencia de lo que ocurre
 * con el evento de pedido creado.
 *
 * Aplica el mismo contrato de confirmacion manual que el resto de los
 * consumidores, con su propia salida a la DLQ.
 */
@Component
public class AlertaStockListener {

    private static final Logger log = LoggerFactory.getLogger(AlertaStockListener.class);
    private static final String DOMINIO = "ALERTAS";

    private final RegistroProcesoService bitacora;

    public AlertaStockListener(RegistroProcesoService bitacora) {
        this.bitacora = bitacora;
    }

    @RabbitListener(queues = "${app.rabbit.queues.alertas-stock}")
    public void recibirAlerta(@Payload AlertaStockEvento alerta,
                              @Header(AmqpHeaders.DELIVERY_TAG) long etiquetaEntrega,
                              @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean reentregado,
                              Channel canal) throws IOException {

        boolean esReintento = Boolean.TRUE.equals(reentregado);

        log.info("[{}] Alerta recibida. pedidoId={} lineas={} umbral={} reintento={}",
                DOMINIO, alerta.pedidoId(),
                alerta.lineas() == null ? 0 : alerta.lineas().size(),
                alerta.umbral(), esReintento);

        try {
            String detalle = alerta.lineas().stream()
                    .map(l -> l.nombre() + " quedo en " + l.stockResultante())
                    .collect(Collectors.joining("; "));

            log.warn("[{}] REPOSICION REQUERIDA. pedidoId={} -> {}",
                    DOMINIO, alerta.pedidoId(), detalle);

            bitacora.registrar(DOMINIO, alerta.pedidoId(), null, "ALERTA", detalle);

            canal.basicAck(etiquetaEntrega, false);
            log.info("[{}] ACK enviado. pedidoId={}", DOMINIO, alerta.pedidoId());

        } catch (ErrorNoRecuperableException e) {
            log.error("[{}] Error NO recuperable. NACK sin reencolar -> DLQ. pedidoId={} causa={}",
                    DOMINIO, alerta.pedidoId(), e.getMessage());
            canal.basicNack(etiquetaEntrega, false, false);

        } catch (ErrorRecuperableException e) {
            if (!esReintento) {
                log.warn("[{}] Error recuperable, primer intento. NACK con reencolado. pedidoId={} causa={}",
                        DOMINIO, alerta.pedidoId(), e.getMessage());
                canal.basicNack(etiquetaEntrega, false, true);
            } else {
                log.error("[{}] Error recuperable persistente. NACK sin reencolar -> DLQ. pedidoId={} causa={}",
                        DOMINIO, alerta.pedidoId(), e.getMessage());
                canal.basicNack(etiquetaEntrega, false, false);
            }

        } catch (Exception e) {
            log.error("[{}] Error inesperado. NACK sin reencolar -> DLQ. pedidoId={}",
                    DOMINIO, alerta.pedidoId(), e);
            canal.basicNack(etiquetaEntrega, false, false);
        }
    }
}
