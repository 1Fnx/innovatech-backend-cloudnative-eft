package cl.innovatech.productos.messaging.evento;

import java.util.List;

/**
 * Aviso de que una o mas lineas de un pedido dejaron el producto por debajo del
 * umbral de reposicion.
 *
 * A diferencia del movimiento de inventario, que describe lo que ocurrio y le
 * interesa a varios consumidores, esta alerta es una instruccion dirigida a un
 * unico destinatario. Por eso viaja por un exchange de tipo direct, con
 * coincidencia exacta de clave, y no por el exchange de tipo topic.
 *
 * @param umbral valor con el que se evaluo la condicion, para que el receptor
 *               pueda explicar el aviso sin consultar la configuracion de
 *               ms-productos
 */
public record AlertaStockEvento(
        String eventoId,
        Long pedidoId,
        int umbral,
        List<Linea> lineas,
        String fecha) {

    public record Linea(
            Long productoId,
            String nombre,
            Integer stockResultante) {
    }
}
