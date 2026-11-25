package cl.innovatech.notificaciones.config;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topologia de mensajeria del microservicio de Notificaciones.
 *
 * Cada ruta de mensajeria que atiende este servicio queda declarada de forma
 * explicita, junto con su salida hacia la DLQ correspondiente.
 *
 *  Flujo 1 - Avisar al cliente
 *     innovatech.pedidos.exchange --(pedido.creado)--> pedidos.notificaciones.queue
 *     si falla definitivamente:
 *     innovatech.dlx --(dlq.notificaciones)--> pedidos.notificaciones.dlq
 *
 *  Flujo 2 - Emitir la boleta
 *     innovatech.pedidos.exchange --(pedido.creado)--> pedidos.documentos.queue
 *     si falla definitivamente:
 *     innovatech.dlx --(dlq.documentos)--> pedidos.documentos.dlq
 *
 *  Flujo 3 - Reponer stock
 *     innovatech.alertas.direct --(alerta.stock.bajo)--> alertas.stock.queue
 *     si falla definitivamente:
 *     innovatech.dlx --(dlq.alertas)--> alertas.stock.dlq
 *
 * Observacion importante: un mismo evento "pedido.creado" llega a DOS colas
 * porque existen DOS bindings con esa misma routing key. El broker entrega una
 * copia a cada cola; no se trata de dos consumidores sobre una misma cola.
 *
 * Los dos primeros flujos salen de un exchange topic, donde la clave se compara
 * contra un patron y el mismo mensaje puede copiarse a varias colas. El tercero
 * sale de un exchange direct, donde la comparacion es exacta y el destino es
 * uno solo. Los dos tipos conviven porque resuelven problemas distintos.
 */
@Configuration
public class RabbitTopologiaConfig {

    private final MensajeriaProperties propiedades;

    public RabbitTopologiaConfig(MensajeriaProperties propiedades) {
        this.propiedades = propiedades;
    }

    // ----------------------------- Exchanges --------------------------------

    @Bean
    public TopicExchange pedidosExchange() {
        return new TopicExchange(propiedades.getExchanges().getPedidos(), true, false);
    }

    @Bean
    public TopicExchange deadLetterExchange() {
        return new TopicExchange(propiedades.getExchanges().getDlx(), true, false);
    }

    /** Exchange de avisos operativos: enrutamiento por clave exacta. */
    @Bean
    public DirectExchange alertasExchange() {
        return new DirectExchange(propiedades.getExchanges().getAlertas(), true, false);
    }

    // ------------------- Caso de uso 1: notificaciones ----------------------

    /**
     * Cola de notificaciones. Los dos argumentos "x-dead-letter-*" son los que
     * le indican a RabbitMQ a donde republicar el mensaje cuando esta cola lo
     * descarta (rechazo sin reencolar, expiracion por TTL o limite de longitud).
     */
    @Bean
    public Queue notificacionesQueue() {
        return QueueBuilder.durable(propiedades.getQueues().getNotificaciones())
                .withArgument("x-dead-letter-exchange", propiedades.getExchanges().getDlx())
                .withArgument("x-dead-letter-routing-key",
                        propiedades.getRoutingKeys().getDlqNotificaciones())
                .build();
    }

    @Bean
    public Queue notificacionesDlq() {
        return QueueBuilder.durable(propiedades.getQueues().getNotificacionesDlq()).build();
    }

    @Bean
    public Binding bindingNotificaciones(Queue notificacionesQueue, TopicExchange pedidosExchange) {
        return BindingBuilder.bind(notificacionesQueue).to(pedidosExchange)
                .with(propiedades.getRoutingKeys().getPedidoCreado());
    }

    @Bean
    public Binding bindingNotificacionesDlq(Queue notificacionesDlq, TopicExchange deadLetterExchange) {
        return BindingBuilder.bind(notificacionesDlq).to(deadLetterExchange)
                .with(propiedades.getRoutingKeys().getDlqNotificaciones());
    }

    // --------------------- Caso de uso 2: documentos ------------------------

    @Bean
    public Queue documentosQueue() {
        return QueueBuilder.durable(propiedades.getQueues().getDocumentos())
                .withArgument("x-dead-letter-exchange", propiedades.getExchanges().getDlx())
                .withArgument("x-dead-letter-routing-key",
                        propiedades.getRoutingKeys().getDlqDocumentos())
                .build();
    }

    @Bean
    public Queue documentosDlq() {
        return QueueBuilder.durable(propiedades.getQueues().getDocumentosDlq()).build();
    }

    @Bean
    public Binding bindingDocumentos(Queue documentosQueue, TopicExchange pedidosExchange) {
        return BindingBuilder.bind(documentosQueue).to(pedidosExchange)
                .with(propiedades.getRoutingKeys().getPedidoCreado());
    }

    @Bean
    public Binding bindingDocumentosDlq(Queue documentosDlq, TopicExchange deadLetterExchange) {
        return BindingBuilder.bind(documentosDlq).to(deadLetterExchange)
                .with(propiedades.getRoutingKeys().getDlqDocumentos());
    }

    // ------------------ Caso de uso 3: alertas de stock ---------------------

    @Bean
    public Queue alertasStockQueue() {
        return QueueBuilder.durable(propiedades.getQueues().getAlertasStock())
                .withArgument("x-dead-letter-exchange", propiedades.getExchanges().getDlx())
                .withArgument("x-dead-letter-routing-key",
                        propiedades.getRoutingKeys().getDlqAlertas())
                .build();
    }

    @Bean
    public Queue alertasStockDlq() {
        return QueueBuilder.durable(propiedades.getQueues().getAlertasStockDlq()).build();
    }

    /**
     * Binding sobre el exchange direct. La clave se escribe completa porque
     * este tipo de exchange no admite comodines: o coincide exactamente, o el
     * mensaje no llega a la cola.
     */
    @Bean
    public Binding bindingAlertasStock(Queue alertasStockQueue, DirectExchange alertasExchange) {
        return BindingBuilder.bind(alertasStockQueue).to(alertasExchange)
                .with(propiedades.getRoutingKeys().getAlertaStockBajo());
    }

    @Bean
    public Binding bindingAlertasStockDlq(Queue alertasStockDlq, TopicExchange deadLetterExchange) {
        return BindingBuilder.bind(alertasStockDlq).to(deadLetterExchange)
                .with(propiedades.getRoutingKeys().getDlqAlertas());
    }
}
