package cl.innovatech.adminrabbit.config;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Unica clase de este microservicio que conoce la infraestructura de RabbitMQ.
 *
 * RabbitAdmin es el componente de Spring AMQP que permite administrar recursos
 * del broker (colas, exchanges y bindings) de forma programatica.
 *
 * Se desactiva la declaracion automatica al arrancar porque este servicio no
 * declara una topologia propia: su proposito es administrar la que se le pida
 * a traves de la API REST.
 */
@Configuration
public class RabbitAdminConfig {

    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        RabbitAdmin admin = new RabbitAdmin(connectionFactory);
        admin.setAutoStartup(true);
        admin.setIgnoreDeclarationExceptions(false);
        return admin;
    }
}
