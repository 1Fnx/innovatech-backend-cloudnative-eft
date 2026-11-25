package cl.innovatech.notificaciones.messaging.listener;

import cl.innovatech.notificaciones.service.RegistroProcesoService;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Auditoria de las Dead Letter Queues del servicio.
 *
 * No reintenta ni reprocesa: su unica tarea es dejar constancia de que llego un
 * mensaje a cuarentena, con que causa y desde que cola, para que el equipo pueda
 * diagnosticarlo. Se confirma siempre con ACK porque la auditoria si se completo.
 *
 * El mensaje se recibe como Message "en crudo" y no como objeto, porque lo que
 * interesa aqui son los encabezados de dead-lettering y el contenido original
 * tal cual viajo, aunque este mal formado.
 *
 * El arranque de los tres consumidores depende de app.rabbit.auditoria-dlq.
 * Dejarlo en false permite acumular mensajes en las DLQ para inspeccionarlas
 * desde la consola del broker sin que nadie las vacie.
 */
@Component
public class DeadLetterListener {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterListener.class);

    private final RegistroProcesoService bitacora;

    public DeadLetterListener(RegistroProcesoService bitacora) {
        this.bitacora = bitacora;
    }

    @RabbitListener(queues = "${app.rabbit.queues.notificaciones-dlq}",
            autoStartup = "${app.rabbit.auditoria-dlq:true}")
    public void auditarDlqNotificaciones(Message mensaje,
                                         @Header(AmqpHeaders.DELIVERY_TAG) long etiquetaEntrega,
                                         Channel canal) throws IOException {
        auditar("DLQ-NOTIFICACIONES", mensaje, etiquetaEntrega, canal);
    }

    @RabbitListener(queues = "${app.rabbit.queues.documentos-dlq}",
            autoStartup = "${app.rabbit.auditoria-dlq:true}")
    public void auditarDlqDocumentos(Message mensaje,
                                     @Header(AmqpHeaders.DELIVERY_TAG) long etiquetaEntrega,
                                     Channel canal) throws IOException {
        auditar("DLQ-DOCUMENTOS", mensaje, etiquetaEntrega, canal);
    }

    @RabbitListener(queues = "${app.rabbit.queues.alertas-stock-dlq}",
            autoStartup = "${app.rabbit.auditoria-dlq:true}")
    public void auditarDlqAlertas(Message mensaje,
                                  @Header(AmqpHeaders.DELIVERY_TAG) long etiquetaEntrega,
                                  Channel canal) throws IOException {
        auditar("DLQ-ALERTAS", mensaje, etiquetaEntrega, canal);
    }

    private void auditar(String origen, Message mensaje, long etiquetaEntrega, Channel canal)
            throws IOException {

        MetadatosDlq.Resumen resumen = MetadatosDlq.leer(mensaje);
        String contenido = new String(mensaje.getBody(), StandardCharsets.UTF_8);

        log.error("=================== MENSAJE EN CUARENTENA ===================");
        log.error("[{}] Se recibio un mensaje no entregado.", origen);
        log.error("[{}] Metadatos de dead-lettering: {}", origen, resumen);
        log.error("[{}] Contenido original: {}", origen, contenido);
        log.error("=============================================================");

        bitacora.registrar(origen, null, null, "DLQ",
                resumen + " | payload=" + contenido);

        // Se confirma: la tarea de auditar el mensaje si se completo con exito.
        canal.basicAck(etiquetaEntrega, false);
    }
}
