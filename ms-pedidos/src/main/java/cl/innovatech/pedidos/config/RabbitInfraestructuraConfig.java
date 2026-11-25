package cl.innovatech.pedidos.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Infraestructura tecnica de la mensajeria: conversor de mensajes, plantilla de
 * publicacion y fabrica de contenedores de consumidores.
 *
 * Toda la plomeria de AMQP queda contenida en esta clase. Los servicios de
 * dominio reciben los componentes ya construidos por inyeccion y en ningun
 * momento manipulan la conexion ni el canal por su cuenta.
 */
@Configuration
public class RabbitInfraestructuraConfig {

    private static final Logger log = LoggerFactory.getLogger(RabbitInfraestructuraConfig.class);

    /**
     * Convierte los objetos Java a JSON y viceversa.
     *
     * Se usa TypePrecedence.INFERRED para que el consumidor deduzca la clase a
     * partir del parametro del metodo anotado y no del encabezado __TypeId__.
     * Sin esto, cada microservicio exigiria tener la clase del emisor con el
     * mismo nombre de paquete, lo que acoplaria los servicios entre si.
     */
    @Bean
    public Jackson2JsonMessageConverter jsonMessageConverter(ObjectMapper objectMapper) {
        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter(objectMapper);
        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
        typeMapper.setTrustedPackages("*");
        typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.INFERRED);
        converter.setJavaTypeMapper(typeMapper);
        return converter;
    }

    /**
     * Plantilla de publicacion. Se activan los "publisher confirms" para saber
     * si el broker acepto realmente el mensaje y los "returns" para detectar
     * mensajes que no pudieron enrutarse a ninguna cola.
     */
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory,
                                         Jackson2JsonMessageConverter converter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(converter);
        template.setMandatory(true);

        template.setConfirmCallback((correlation, aceptado, causa) -> {
            if (aceptado) {
                log.debug("[PUBLICACION] El broker confirmo la recepcion del mensaje. id={}",
                        correlation != null ? correlation.getId() : "sin-id");
            } else {
                log.error("[PUBLICACION] El broker RECHAZO el mensaje. id={} causa={}",
                        correlation != null ? correlation.getId() : "sin-id", causa);
            }
        });

        template.setReturnsCallback(retorno ->
                log.error("[PUBLICACION] Mensaje NO enrutable (ninguna cola coincide). " +
                                "exchange={} routingKey={} motivo={}",
                        retorno.getExchange(), retorno.getRoutingKey(), retorno.getReplyText()));

        return template;
    }

    /**
     * Fabrica de contenedores para los consumidores.
     *
     * Con AcknowledgeMode.MANUAL cada consumidor debe confirmar (ACK) o
     * rechazar (NACK) el mensaje de forma explicita. Mientras no lo haga, el
     * mensaje sigue reservado para esa entrega, de modo que una caida del
     * proceso no lo da por procesado.
     */
    @Bean(name = "rabbitListenerContainerFactory")
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory, Jackson2JsonMessageConverter converter) {

        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(converter);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setPrefetchCount(1);
        factory.setDefaultRequeueRejected(false);
        return factory;
    }
}
