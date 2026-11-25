package cl.innovatech.productos.messaging.evento;

import java.util.List;

/**
 * Contrato del mensaje que viaja por RabbitMQ cuando se crea un pedido.
 *
 * Se publica una sola vez en el exchange de pedidos y el broker entrega una
 * copia a cada cola interesada (notificaciones, documentos e inventario).
 *
 * El campo "simulacion" es un conmutador de pruebas: permite forzar un error
 * recuperable o uno no recuperable para ejercitar las rutas de reintento y de
 * dead-lettering sin depender de que falle un sistema externo.
 */
public record PedidoEvento(
        String eventoId,
        String tipoEvento,
        Long pedidoId,
        String cliente,
        String email,
        Double total,
        List<Item> items,
        String simulacion,
        String fecha
) {
    public record Item(Long productoId, String nombre, Integer cantidad, Double precio) {}

    /** Valores admitidos para el campo "simulacion". */
    public static final String SIM_NINGUNA = "NINGUNA";
    public static final String SIM_ERROR_RECUPERABLE = "ERROR_RECUPERABLE";
    public static final String SIM_ERROR_NO_RECUPERABLE = "ERROR_NO_RECUPERABLE";
}
