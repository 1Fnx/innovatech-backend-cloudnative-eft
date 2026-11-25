package cl.innovatech.adminrabbit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Microservicio ADMINISTRADOR de RabbitMQ.
 *
 * Expone una API REST que permite crear y eliminar colas, exchanges y bindings
 * en tiempo de ejecucion, sin reiniciar ningun servicio y sin entrar a la
 * consola web del broker.
 */
@SpringBootApplication
public class MsAdminRabbitApplication {
    public static void main(String[] args) {
        SpringApplication.run(MsAdminRabbitApplication.class, args);
    }
}
