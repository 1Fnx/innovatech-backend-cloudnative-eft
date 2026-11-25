package cl.innovatech.pedidos.messaging.producer;

import cl.innovatech.pedidos.config.MensajeriaProperties;
import cl.innovatech.pedidos.messaging.evento.PedidoEvento;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

/**
 * Implementacion del productor sobre RabbitMQ.
 *
 * Es la unica clase del microservicio que conoce RabbitTemplate. Toma el nombre
 * del exchange y la routing key desde la configuracion centralizada, nunca de
 * literales escritos en el codigo.
 */
@Component
public class PedidoEventPublisherRabbit implements PedidoEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(PedidoEventPublisherRabbit.class);

    private final RabbitTemplate rabbitTemplate;
    private final MensajeriaProperties propiedades;

    public PedidoEventPublisherRabbit(RabbitTemplate rabbitTemplate, MensajeriaProperties propiedades) {
        this.rabbitTemplate = rabbitTemplate;
        this.propiedades = propiedades;
    }

    @Override
    public void publicarPedidoCreado(PedidoEvento evento) {
        String exchange = propiedades.getExchanges().getPedidos();
        String routingKey = propiedades.getRoutingKeys().getPedidoCreado();

        log.info("[PRODUCTOR] Publicando evento. exchange={} routingKey={} pedidoId={} simulacion={}",
                exchange, routingKey, evento.pedidoId(), evento.simulacion());

        rabbitTemplate.convertAndSend(exchange, routingKey, evento,
                new CorrelationData(evento.eventoId()));

        log.info("[PRODUCTOR] Evento entregado al broker. eventoId={}", evento.eventoId());
    }
}
