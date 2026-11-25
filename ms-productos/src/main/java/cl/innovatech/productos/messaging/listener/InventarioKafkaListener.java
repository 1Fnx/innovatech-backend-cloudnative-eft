package cl.innovatech.productos.messaging.listener;

import cl.innovatech.productos.exception.ErrorNoRecuperableException;
import cl.innovatech.productos.messaging.evento.MovimientoInventarioEvento;
import cl.innovatech.productos.messaging.evento.PedidoEvento;
import cl.innovatech.productos.messaging.producer.MovimientoInventarioPublisher;
import cl.innovatech.productos.service.InventarioService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Consumo del topico de pedidos: descuenta stock y publica el movimiento.
 *
 * Comparte la logica de negocio con InventarioListener (AMQP), porque los dos
 * delegan en InventarioService. La diferencia esta en el manejo de errores: en
 * AMQP el consumidor confirma a mano con basicAck o basicNack; aqui basta con
 * lanzar o no lanzar, y quien decide reintentar o derivar al topico .DLT es el
 * DefaultErrorHandler.
 */
@Component
public class InventarioKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(InventarioKafkaListener.class);
    private static final String DOMINIO = "INVENTARIO KAFKA";

    private final InventarioService servicio;
    private final MovimientoInventarioPublisher publicador;

    public InventarioKafkaListener(InventarioService servicio,
                                   MovimientoInventarioPublisher publicador) {
        this.servicio = servicio;
        this.publicador = publicador;
    }

    /**
     * Particion, offset y clave se reciben como cabeceras para dejarlos en el
     * log: son la direccion del registro dentro del topico.
     *
     * No se atrapan excepciones a proposito. Dejarlas salir es la forma de
     * avisarle al contenedor que el procesamiento fallo; atraparlas haria que
     * el offset avanzara y el evento se perdiera en silencio.
     */
    @KafkaListener(
            topics = "${app.kafka.topics.pedidos-eventos}",
            groupId = "${app.kafka.grupos.inventario}")
    public void recibirPedidoCreado(@Payload PedidoEvento evento,
                                    @Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
                                    @Header(KafkaHeaders.OFFSET) long offset,
                                    @Header(KafkaHeaders.RECEIVED_KEY) String clave) {

        log.info("[{}] Evento recibido. particion={} offset={} clave={} pedidoId={} simulacion={}",
                DOMINIO, particion, offset, clave, evento.pedidoId(), evento.simulacion());

        validar(evento);

        List<MovimientoInventarioEvento.Linea> lineas = servicio.descontarStock(evento);

        publicador.publicarMovimiento(evento.pedidoId(), lineas, servicio.hayStockBajo(lineas));

        log.info("[{}] Procesado. offset={} lineas={} -> movimiento publicado",
                DOMINIO, offset, lineas.size());
    }

    /**
     * Un payload incompleto es un defecto permanente: se lanza la excepcion no
     * reintentable para que vaya directo al topico .DLT sin bloquear la
     * particion durante los reintentos.
     */
    private void validar(PedidoEvento evento) {
        if (evento.pedidoId() == null) {
            throw new ErrorNoRecuperableException("El evento llego sin identificador de pedido");
        }
        if (evento.items() == null || evento.items().isEmpty()) {
            throw new ErrorNoRecuperableException(
                    "El pedido " + evento.pedidoId() + " no trae lineas de detalle");
        }
    }
}
