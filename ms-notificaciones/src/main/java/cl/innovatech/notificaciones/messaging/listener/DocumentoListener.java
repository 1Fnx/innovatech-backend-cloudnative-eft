package cl.innovatech.notificaciones.messaging.listener;

import cl.innovatech.notificaciones.exception.ErrorNoRecuperableException;
import cl.innovatech.notificaciones.exception.ErrorRecuperableException;
import cl.innovatech.notificaciones.messaging.evento.PedidoEvento;
import cl.innovatech.notificaciones.service.DocumentoService;
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

/**
 * Consumidor del dominio de documentos (emision de boletas).
 *
 * Escucha su propia cola, distinta de la de notificaciones, aunque ambas
 * reaccionen al mismo evento de negocio. Aplica el mismo esquema explicito de
 * confirmacion: ACK al procesar, reintento unico ante fallo transitorio y envio
 * a la DLQ cuando el fallo es definitivo.
 */
@Component
public class DocumentoListener {

    private static final Logger log = LoggerFactory.getLogger(DocumentoListener.class);
    private static final String DOMINIO = "DOCUMENTOS";

    private final DocumentoService servicio;
    private final RegistroProcesoService bitacora;

    public DocumentoListener(DocumentoService servicio, RegistroProcesoService bitacora) {
        this.servicio = servicio;
        this.bitacora = bitacora;
    }

    @RabbitListener(queues = "${app.rabbit.queues.documentos}")
    public void recibirPedidoCreado(@Payload PedidoEvento evento,
                                    @Header(AmqpHeaders.DELIVERY_TAG) long etiquetaEntrega,
                                    @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean reentregado,
                                    Channel canal) throws IOException {

        boolean esReintento = Boolean.TRUE.equals(reentregado);

        log.info("[{}] Mensaje recibido. pedidoId={} reintento={}",
                DOMINIO, evento.pedidoId(), esReintento);

        try {
            servicio.generarBoleta(evento);

            canal.basicAck(etiquetaEntrega, false);
            log.info("[{}] ACK enviado. Boleta emitida. pedidoId={}", DOMINIO, evento.pedidoId());

        } catch (ErrorNoRecuperableException e) {
            log.error("[{}] Error NO recuperable. NACK sin reencolar -> DLQ. pedidoId={} causa={}",
                    DOMINIO, evento.pedidoId(), e.getMessage());
            bitacora.registrar(DOMINIO, evento.pedidoId(), evento.cliente(), "DLQ",
                    "Error no recuperable: " + e.getMessage());
            canal.basicNack(etiquetaEntrega, false, false);

        } catch (ErrorRecuperableException e) {
            if (!esReintento) {
                log.warn("[{}] Error recuperable, primer intento. NACK con reencolado. pedidoId={} causa={}",
                        DOMINIO, evento.pedidoId(), e.getMessage());
                bitacora.registrar(DOMINIO, evento.pedidoId(), evento.cliente(), "REINTENTO",
                        "Error recuperable: " + e.getMessage());
                canal.basicNack(etiquetaEntrega, false, true);
            } else {
                log.error("[{}] Error recuperable persistente. NACK sin reencolar -> DLQ. pedidoId={} causa={}",
                        DOMINIO, evento.pedidoId(), e.getMessage());
                bitacora.registrar(DOMINIO, evento.pedidoId(), evento.cliente(), "DLQ",
                        "Error recuperable persistente: " + e.getMessage());
                canal.basicNack(etiquetaEntrega, false, false);
            }

        } catch (Exception e) {
            log.error("[{}] Error inesperado. NACK sin reencolar -> DLQ. pedidoId={}",
                    DOMINIO, evento.pedidoId(), e);
            bitacora.registrar(DOMINIO, evento.pedidoId(), evento.cliente(), "DLQ",
                    "Error inesperado: " + e.getClass().getSimpleName());
            canal.basicNack(etiquetaEntrega, false, false);
        }
    }
}
