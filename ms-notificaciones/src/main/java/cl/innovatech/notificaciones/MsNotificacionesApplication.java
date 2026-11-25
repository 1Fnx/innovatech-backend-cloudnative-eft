package cl.innovatech.notificaciones;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Microservicio de Notificaciones y Documentos.
 *
 * Rol dentro de la mensajeria: CONSUMIDOR.
 * Atiende dos dominios funcionales distintos sobre el mismo evento de negocio:
 *   - Notificaciones: avisa al cliente que su pedido fue recibido.
 *   - Documentos: genera la boleta del pedido.
 * Ademas audita las dos Dead Letter Queues asociadas a esos dominios.
 */
@SpringBootApplication
public class MsNotificacionesApplication {
    public static void main(String[] args) {
        SpringApplication.run(MsNotificacionesApplication.class, args);
    }
}
