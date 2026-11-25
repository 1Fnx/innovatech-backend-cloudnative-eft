package cl.innovatech.productos.service;

import cl.innovatech.productos.domain.Producto;
import cl.innovatech.productos.exception.ErrorNoRecuperableException;
import cl.innovatech.productos.exception.ErrorRecuperableException;
import cl.innovatech.productos.messaging.evento.MovimientoInventarioEvento;
import cl.innovatech.productos.messaging.evento.PedidoEvento;
import cl.innovatech.productos.repository.ProductoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

/**
 * Dominio funcional INVENTARIO: descuenta el stock de los productos vendidos.
 *
 * Es logica de negocio pura: no conoce colas ni confirmaciones AMQP. Distingue
 * entre un fallo transitorio (merece reintento) y uno definitivo (no lo merece)
 * lanzando una u otra excepcion.
 */
@Service
public class InventarioService {

    private static final Logger log = LoggerFactory.getLogger(InventarioService.class);

    private final ProductoRepository repositorio;

    /** Regla de negocio: vive aqui, no en el consumidor que recibe el aviso. */
    @Value("${app.inventario.umbral-stock-bajo:10}")
    private int umbralStockBajo;

    public InventarioService(ProductoRepository repositorio) {
        this.repositorio = repositorio;
    }

    /**
     * Descuenta el stock y devuelve el detalle de lo movido, para que quien
     * encadene la siguiente etapa no tenga que volver a consultar la base.
     */
    @Transactional
    public List<MovimientoInventarioEvento.Linea> descontarStock(PedidoEvento evento) {

        List<MovimientoInventarioEvento.Linea> lineas = new ArrayList<>();

        if (evento.items() == null || evento.items().isEmpty()) {
            throw new ErrorNoRecuperableException(
                    "El pedido " + evento.pedidoId() + " no trae lineas de detalle");
        }

        if (PedidoEvento.SIM_ERROR_RECUPERABLE.equals(evento.simulacion())) {
            throw new ErrorRecuperableException(
                    "La base de datos de inventario esta momentaneamente ocupada (fallo transitorio simulado)");
        }
        if (PedidoEvento.SIM_ERROR_NO_RECUPERABLE.equals(evento.simulacion())) {
            throw new ErrorNoRecuperableException(
                    "El pedido referencia un producto inexistente (fallo permanente simulado)");
        }

        for (PedidoEvento.Item item : evento.items()) {
            Producto producto = repositorio.findById(item.productoId())
                    .orElseThrow(() -> new ErrorNoRecuperableException(
                            "El producto " + item.productoId() + " no existe en el catalogo"));

            int stockResultante = producto.getStock() - item.cantidad();
            if (stockResultante < 0) {
                throw new ErrorNoRecuperableException(
                        "Stock insuficiente para el producto '" + producto.getNombre()
                                + "'. Disponible=" + producto.getStock()
                                + ", solicitado=" + item.cantidad());
            }

            int stockAnterior = producto.getStock();
            producto.setStock(stockResultante);
            repositorio.save(producto);

            lineas.add(new MovimientoInventarioEvento.Linea(
                    producto.getId(), producto.getNombre(),
                    item.cantidad(), stockAnterior, stockResultante));

            log.info("[INVENTARIO] Stock actualizado. producto='{}' descontado={} restante={}",
                    producto.getNombre(), item.cantidad(), stockResultante);
        }

        return lineas;
    }

    /** Indica si alguna de las lineas dejo el producto bajo el umbral. */
    public boolean hayStockBajo(List<MovimientoInventarioEvento.Linea> lineas) {
        return lineas.stream().anyMatch(l -> l.stockResultante() < umbralStockBajo);
    }
}
