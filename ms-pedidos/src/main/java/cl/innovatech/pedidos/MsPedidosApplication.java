package cl.innovatech.pedidos;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Microservicio de Pedidos.
 *
 * Rol dentro de la mensajeria: PRODUCTOR.
 * Cuando se crea un pedido, publica una sola vez el evento "pedido.creado" en
 * el exchange de pedidos. El broker se encarga de entregar una copia a cada
 * cola interesada, sin que este microservicio conozca a sus consumidores.
 */
@SpringBootApplication
public class MsPedidosApplication {
    public static void main(String[] args) {
        SpringApplication.run(MsPedidosApplication.class, args);
    }
}
