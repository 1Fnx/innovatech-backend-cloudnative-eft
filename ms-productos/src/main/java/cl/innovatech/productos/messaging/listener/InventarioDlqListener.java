package cl.innovatech.productos.messaging.listener;

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
 * Auditoria de la Dead Letter Queue del dominio INVENTARIO.
 * Registra en el log la causa del dead-lettering y el contenido original del
 * mensaje, de modo que el equipo pueda diagnosticar por que no se proceso.
 */
@Component
public class InventarioDlqListener {

    private static final Logger log = LoggerFactory.getLogger(InventarioDlqListener.class);

    @RabbitListener(queues = "${app.rabbit.queues.inventario-dlq}",
            autoStartup = "${app.rabbit.auditoria-dlq:true}")
    public void auditar(Message mensaje,
                        @Header(AmqpHeaders.DELIVERY_TAG) long etiquetaEntrega,
                        Channel canal) throws IOException {

        MetadatosDlq.Resumen resumen = MetadatosDlq.leer(mensaje);
        String contenido = new String(mensaje.getBody(), StandardCharsets.UTF_8);

        log.error("=================== MENSAJE EN CUARENTENA ===================");
        log.error("[DLQ-INVENTARIO] Se recibio un mensaje no entregado.");
        log.error("[DLQ-INVENTARIO] Metadatos de dead-lettering: {}", resumen);
        log.error("[DLQ-INVENTARIO] Contenido original: {}", contenido);
        log.error("=============================================================");

        canal.basicAck(etiquetaEntrega, false);
    }
}
