package cl.innovatech.productos.config;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topologia de mensajeria del microservicio de Productos.
 *
 * Este servicio declara unicamente la cola que consume y su salida a la DLQ.
 *
 *  Flujo 1 - Descontar stock al crear un pedido
 *     innovatech.pedidos.exchange --(pedido.creado)--> pedidos.inventario.queue
 *     si falla definitivamente:
 *     innovatech.dlx --(dlq.inventario)--> pedidos.inventario.dlq
 *
 *  Flujo 2 - Avisar que un producto quedo bajo el umbral
 *     este servicio publica en innovatech.alertas.direct con la clave exacta
 *     alerta.stock.bajo. La cola de destino y su binding los declara el
 *     consumidor, igual que en el resto de la topologia.
 */
@Configuration
public class RabbitTopologiaConfig {

    private final MensajeriaProperties propiedades;

    public RabbitTopologiaConfig(MensajeriaProperties propiedades) {
        this.propiedades = propiedades;
    }

    @Bean
    public TopicExchange pedidosExchange() {
        return new TopicExchange(propiedades.getExchanges().getPedidos(), true, false);
    }

    @Bean
    public TopicExchange deadLetterExchange() {
        return new TopicExchange(propiedades.getExchanges().getDlx(), true, false);
    }

    /**
     * Exchange de avisos operativos. Es de tipo direct porque el enrutamiento
     * se resuelve por coincidencia exacta de la clave, no por patrones.
     */
    @Bean
    public DirectExchange alertasExchange() {
        return new DirectExchange(propiedades.getExchanges().getAlertas(), true, false);
    }

    /** Cola de inventario, con su Dead Letter Exchange y routing key asociados. */
    @Bean
    public Queue inventarioQueue() {
        return QueueBuilder.durable(propiedades.getQueues().getInventario())
                .withArgument("x-dead-letter-exchange", propiedades.getExchanges().getDlx())
                .withArgument("x-dead-letter-routing-key",
                        propiedades.getRoutingKeys().getDlqInventario())
                .build();
    }

    @Bean
    public Queue inventarioDlq() {
        return QueueBuilder.durable(propiedades.getQueues().getInventarioDlq()).build();
    }

    @Bean
    public Binding bindingInventario(Queue inventarioQueue, TopicExchange pedidosExchange) {
        return BindingBuilder.bind(inventarioQueue).to(pedidosExchange)
                .with(propiedades.getRoutingKeys().getPedidoCreado());
    }

    @Bean
    public Binding bindingInventarioDlq(Queue inventarioDlq, TopicExchange deadLetterExchange) {
        return BindingBuilder.bind(inventarioDlq).to(deadLetterExchange)
                .with(propiedades.getRoutingKeys().getDlqInventario());
    }
}
