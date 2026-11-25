package cl.innovatech.productos;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Microservicio de Productos.
 *
 * Mantiene el catalogo de la tienda (API REST protegida con JWT) y, ademas,
 * actua como CONSUMIDOR del dominio INVENTARIO: cuando se crea un pedido
 * descuenta el stock de forma asincrona, sin bloquear al cliente.
 */
@SpringBootApplication
public class MsProductosApplication {
    public static void main(String[] args) {
        SpringApplication.run(MsProductosApplication.class, args);
    }
}
