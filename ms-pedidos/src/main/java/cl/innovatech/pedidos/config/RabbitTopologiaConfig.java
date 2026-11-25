package cl.innovatech.pedidos.config;

import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topologia de mensajeria declarada por el microservicio de Pedidos.
 *
 * Este servicio actua unicamente como productor, de modo que lo unico que
 * necesita declarar son los dos exchanges. Las colas y sus bindings los declara
 * cada consumidor, que es quien conoce el detalle de su propio flujo.
 *
 * Declarar un exchange es una operacion idempotente: si ya existe con los
 * mismos parametros, RabbitMQ simplemente lo reutiliza.
 *
 *   Flujo que cubre este servicio: "se creo un pedido"
 *     publicacion -> innovatech.pedidos.exchange  (routing key: pedido.creado)
 */
@Configuration
public class RabbitTopologiaConfig {

    private final MensajeriaProperties propiedades;

    public RabbitTopologiaConfig(MensajeriaProperties propiedades) {
        this.propiedades = propiedades;
    }

    /** Exchange de negocio: recibe todos los eventos del dominio de pedidos. */
    @Bean
    public TopicExchange pedidosExchange() {
        return new TopicExchange(propiedades.getExchanges().getPedidos(), true, false);
    }

    /** Dead Letter Exchange: destino de los mensajes que no se pudieron procesar. */
    @Bean
    public TopicExchange deadLetterExchange() {
        return new TopicExchange(propiedades.getExchanges().getDlx(), true, false);
    }
}
