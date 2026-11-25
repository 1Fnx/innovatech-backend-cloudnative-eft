package cl.innovatech.notificaciones.messaging.listener;

import cl.innovatech.notificaciones.exception.ErrorNoRecuperableException;
import cl.innovatech.notificaciones.exception.ErrorRecuperableException;
import cl.innovatech.notificaciones.messaging.evento.PedidoEvento;
import cl.innovatech.notificaciones.service.NotificacionService;
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
 * Consumidor del dominio de notificaciones.
 *
 * El nombre de la cola se resuelve por placeholder contra la configuracion
 * centralizada, de modo que la clase no depende de un literal.
 *
 * La confirmacion es manual y explicita. Hay cuatro rutas de salida
 * diferenciadas:
 *
 *   EXITO                      -> basicAck            (el mensaje se retira)
 *   ERROR TRANSITORIO, 1er int -> basicNack(requeue=true)  (se reintenta)
 *   ERROR TRANSITORIO, 2do int -> basicNack(requeue=false) (se va a la DLQ)
 *   ERROR PERMANENTE           -> basicNack(requeue=false) (se va a la DLQ)
 *
 * Se limita el reintento a un unico reenvio para no provocar un ciclo infinito
 * con un mensaje envenenado.
 */
@Component
public class NotificacionListener {

    private static final Logger log = LoggerFactory.getLogger(NotificacionListener.class);
    private static final String DOMINIO = "NOTIFICACION";

    private final NotificacionService servicio;
    private final RegistroProcesoService bitacora;

    public NotificacionListener(NotificacionService servicio, RegistroProcesoService bitacora) {
        this.servicio = servicio;
        this.bitacora = bitacora;
    }

    @RabbitListener(queues = "${app.rabbit.queues.notificaciones}")
    public void recibirPedidoCreado(@Payload PedidoEvento evento,
                                    @Header(AmqpHeaders.DELIVERY_TAG) long etiquetaEntrega,
                                    @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean reentregado,
                                    Channel canal) throws IOException {

        boolean esReintento = Boolean.TRUE.equals(reentregado);

        log.info("[{}] Mensaje recibido. pedidoId={} cliente={} reintento={}",
                DOMINIO, evento.pedidoId(), evento.cliente(), esReintento);

        try {
            servicio.enviarNotificacion(evento);

            // RUTA 1: todo salio bien -> se confirma y el broker retira el mensaje.
            canal.basicAck(etiquetaEntrega, false);
            log.info("[{}] ACK enviado. El mensaje se procesó correctamente. pedidoId={}",
                    DOMINIO, evento.pedidoId());

        } catch (ErrorNoRecuperableException e) {

            // RUTA 2: reintentar no sirve -> rechazo sin reencolar, va directo a la DLQ.
            log.error("[{}] Error NO recuperable. NACK sin reencolar -> el mensaje viaja a la DLQ. "
                    + "pedidoId={} causa={}", DOMINIO, evento.pedidoId(), e.getMessage());
            bitacora.registrar(DOMINIO, evento.pedidoId(), evento.cliente(), "DLQ",
                    "Error no recuperable: " + e.getMessage());
            canal.basicNack(etiquetaEntrega, false, false);

        } catch (ErrorRecuperableException e) {

            if (!esReintento) {
                // RUTA 3: fallo transitorio en el primer intento -> se reencola una vez.
                log.warn("[{}] Error recuperable en el primer intento. NACK con reencolado "
                        + "-> se volvera a intentar. pedidoId={} causa={}",
                        DOMINIO, evento.pedidoId(), e.getMessage());
                bitacora.registrar(DOMINIO, evento.pedidoId(), evento.cliente(), "REINTENTO",
                        "Error recuperable: " + e.getMessage());
                canal.basicNack(etiquetaEntrega, false, true);
            } else {
                // RUTA 4: el fallo persiste tras el reintento -> se envia a la DLQ.
                log.error("[{}] Error recuperable persistente tras el reintento. NACK sin reencolar "
                        + "-> el mensaje viaja a la DLQ. pedidoId={} causa={}",
                        DOMINIO, evento.pedidoId(), e.getMessage());
                bitacora.registrar(DOMINIO, evento.pedidoId(), evento.cliente(), "DLQ",
                        "Error recuperable persistente: " + e.getMessage());
                canal.basicNack(etiquetaEntrega, false, false);
            }

        } catch (Exception e) {

            // RUTA 5: cualquier fallo no previsto se trata como definitivo.
            log.error("[{}] Error inesperado. NACK sin reencolar -> el mensaje viaja a la DLQ. "
                    + "pedidoId={}", DOMINIO, evento.pedidoId(), e);
            bitacora.registrar(DOMINIO, evento.pedidoId(), evento.cliente(), "DLQ",
                    "Error inesperado: " + e.getClass().getSimpleName() + " - " + e.getMessage());
            canal.basicNack(etiquetaEntrega, false, false);
        }
    }
}
