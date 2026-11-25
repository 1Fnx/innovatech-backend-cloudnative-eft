package cl.innovatech.notificaciones.messaging.listener;

import cl.innovatech.notificaciones.exception.ErrorNoRecuperableException;
import cl.innovatech.notificaciones.messaging.evento.PedidoEvento;
import cl.innovatech.notificaciones.service.NotificacionService;
import cl.innovatech.notificaciones.service.RegistroProcesoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumo del topico de pedidos para avisar al cliente.
 *
 * Usa un grupo propio, distinto del de ms-productos: Kafka guarda un offset por
 * grupo, de modo que ambos microservicios recorren el topico completo. Un grupo
 * compartido repartiria las particiones entre los dos y cada evento llegaria a
 * uno solo, que es lo que se busca entre instancias del mismo microservicio y
 * lo que hay que evitar entre microservicios distintos.
 */
@Component
public class NotificacionKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(NotificacionKafkaListener.class);
    private static final String DOMINIO = "NOTIFICACIONES KAFKA";

    private final NotificacionService servicio;
    private final RegistroProcesoService bitacora;

    public NotificacionKafkaListener(NotificacionService servicio, RegistroProcesoService bitacora) {
        this.servicio = servicio;
        this.bitacora = bitacora;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.pedidos-eventos}",
            groupId = "${app.kafka.grupos.notificaciones}",
            containerFactory = "fabricaPedidos")
    public void recibirPedidoCreado(@Payload PedidoEvento evento,
                                    @Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
                                    @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("[{}] Evento recibido. particion={} offset={} pedidoId={} simulacion={}",
                DOMINIO, particion, offset, evento.pedidoId(), evento.simulacion());

        if (evento.email() == null || evento.email().isBlank()) {
            throw new ErrorNoRecuperableException(
                    "El pedido " + evento.pedidoId() + " no trae correo de destino");
        }

        servicio.enviarNotificacion(evento);

        bitacora.registrar(DOMINIO, evento.pedidoId(), evento.cliente(), "OK",
                "Aviso enviado desde Kafka. particion=" + particion + " offset=" + offset);

        log.info("[{}] Procesado. offset={} destinatario={}", DOMINIO, offset, evento.email());
    }
}
