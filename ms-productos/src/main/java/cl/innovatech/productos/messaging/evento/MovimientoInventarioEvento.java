package cl.innovatech.productos.messaging.evento;

import java.util.List;

/**
 * Movimiento de stock resultante de un pedido. Segunda etapa del pipeline.
 *
 * @param pedidoId     enlaza con el evento que lo origino; es tambien la clave
 *                     del mensaje, de modo que ambas etapas comparten particion
 * @param hayStockBajo calculado por quien tiene el dato, para que los
 *                     consumidores no repitan la regla
 */
public record MovimientoInventarioEvento(
        String eventoId,
        String tipoEvento,
        Long pedidoId,
        List<Linea> lineas,
        boolean hayStockBajo,
        String fecha) {

    /**
     * @param stockAnterior   stock antes del descuento
     * @param stockResultante stock despues. Van los dos para que el consumidor
     *                        reconstruya el movimiento sin consultar la base de
     *                        datos de ms-productos
     */
    public record Linea(
            Long productoId,
            String nombre,
            Integer cantidadDescontada,
            Integer stockAnterior,
            Integer stockResultante) {
    }

    public static final String TIPO_STOCK_DESCONTADO = "STOCK_DESCONTADO";
}
