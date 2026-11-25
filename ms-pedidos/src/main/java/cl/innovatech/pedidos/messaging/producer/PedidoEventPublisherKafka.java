package cl.innovatech.pedidos.messaging.producer;

import cl.innovatech.pedidos.config.KafkaProperties;
import cl.innovatech.pedidos.messaging.evento.PedidoEvento;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Publicacion del evento de pedido en Kafka.
 *
 * Implementa el mismo puerto que PedidoEventPublisherRabbit y convive con el:
 * la cola transporta tareas que se ejecutan una vez, el log transporta hechos
 * que varios interesados leen a su ritmo y pueden releer.
 *
 * La clave del mensaje es el identificador del pedido. Kafka deriva la
 * particion del hash de la clave, de modo que los eventos de un mismo pedido
 * caen siempre en la misma particion y conservan su orden. Sin clave se
 * repartirian entre las tres y podrian procesarse al reves.
 */
@Component
public class PedidoEventPublisherKafka implements PedidoEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(PedidoEventPublisherKafka.class);

    private final KafkaTemplate<String, PedidoEvento> kafkaTemplate;
    private final KafkaProperties propiedades;

    public PedidoEventPublisherKafka(KafkaTemplate<String, PedidoEvento> kafkaTemplate,
                                     KafkaProperties propiedades) {
        this.kafkaTemplate = kafkaTemplate;
        this.propiedades = propiedades;
    }

    @Override
    public void publicarPedidoCreado(PedidoEvento evento) {
        String topic = propiedades.getTopics().getPedidosEventos();
        String clave = String.valueOf(evento.pedidoId());

        log.info("[PRODUCTOR KAFKA] Publicando evento. topic={} clave={} pedidoId={} simulacion={}",
                topic, clave, evento.pedidoId(), evento.simulacion());

        // Devolucion de llamada en lugar de get(): el hilo que atiende la
        // peticion HTTP no espera la confirmacion del broker.
        kafkaTemplate.send(topic, clave, evento)
                .whenComplete((resultado, error) -> {
                    if (error != null) {
                        log.error("[PRODUCTOR KAFKA] No se pudo publicar. eventoId={} causa={}",
                                evento.eventoId(), error.getMessage(), error);
                        return;
                    }
                    registrarConfirmacion(evento, resultado);
                });
    }

    /**
     * Registra la particion y el offset asignados: son la direccion exacta del
     * registro dentro del topico.
     */
    private void registrarConfirmacion(PedidoEvento evento, SendResult<String, PedidoEvento> resultado) {
        log.info("[PRODUCTOR KAFKA] Confirmado por el broker. eventoId={} topic={} particion={} offset={}",
                evento.eventoId(),
                resultado.getRecordMetadata().topic(),
                resultado.getRecordMetadata().partition(),
                resultado.getRecordMetadata().offset());
    }
}
