package cl.innovatech.pedidos.service;

import cl.innovatech.pedidos.domain.Pedido;
import cl.innovatech.pedidos.domain.PedidoItem;
import cl.innovatech.pedidos.dto.CrearPedidoRequest;
import cl.innovatech.pedidos.messaging.evento.PedidoEvento;
import cl.innovatech.pedidos.messaging.producer.PedidoEventPublisher;
import cl.innovatech.pedidos.repository.PedidoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

/**
 * Logica de negocio de los pedidos.
 *
 * No conoce la infraestructura de mensajeria: solo anuncia el hecho a traves
 * del puerto PedidoEventPublisher. Spring inyecta todas las implementaciones
 * disponibles, de modo que agregar o quitar un canal no obliga a tocar esta
 * clase.
 */
@Service
public class PedidoService {

    private static final Logger log = LoggerFactory.getLogger(PedidoService.class);
    private static final DateTimeFormatter FORMATO = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final PedidoRepository repositorio;
    private final List<PedidoEventPublisher> publicadores;

    public PedidoService(PedidoRepository repositorio, List<PedidoEventPublisher> publicadores) {
        this.repositorio = repositorio;
        this.publicadores = publicadores;
    }

    @Transactional(readOnly = true)
    public List<Pedido> listar() {
        return repositorio.findAll();
    }

    @Transactional(readOnly = true)
    public Pedido obtener(Long id) {
        return repositorio.findById(id).orElse(null);
    }

    /**
     * Guarda el pedido y recien entonces anuncia el hecho. Las tareas derivadas
     * (notificar, emitir boleta, descontar stock) ocurren por separado.
     */
    @Transactional
    public Pedido crear(CrearPedidoRequest solicitud) {
        double total = solicitud.items().stream()
                .mapToDouble(i -> i.precio() * i.cantidad())
                .sum();

        Pedido pedido = new Pedido();
        pedido.setCliente(solicitud.cliente());
        pedido.setEmail(solicitud.email());
        pedido.setTotal(total);
        pedido.setEstado("CREADO");
        pedido.setFechaCreacion(LocalDateTime.now().format(FORMATO));
        pedido.setItems(solicitud.items().stream()
                .map(i -> new PedidoItem(i.productoId(), i.nombre(), i.cantidad(), i.precio()))
                .toList());

        Pedido guardado = repositorio.save(pedido);
        log.info("[NEGOCIO] Pedido almacenado. id={} cliente={} total={}",
                guardado.getId(), guardado.getCliente(), guardado.getTotal());

        PedidoEvento evento = construirEvento(guardado, solicitud.simulacion());
        publicarEnTodosLosCanales(evento);
        return guardado;
    }

    /**
     * Publica por cada canal disponible, aislando el fallo de uno del resto: el
     * pedido ya esta guardado y la caida de un broker no debe deshacerlo.
     */
    private void publicarEnTodosLosCanales(PedidoEvento evento) {
        for (PedidoEventPublisher publicador : publicadores) {
            try {
                publicador.publicarPedidoCreado(evento);
            } catch (RuntimeException e) {
                log.error("[NEGOCIO] Fallo al publicar por {}. eventoId={} causa={}",
                        publicador.getClass().getSimpleName(), evento.eventoId(), e.getMessage(), e);
            }
        }
    }

    /** Traduce la entidad de dominio al contrato de mensaje publicado. */
    private PedidoEvento construirEvento(Pedido pedido, String simulacion) {
        List<PedidoEvento.Item> items = pedido.getItems().stream()
                .map(i -> new PedidoEvento.Item(i.getProductoId(), i.getNombre(),
                        i.getCantidad(), i.getPrecio()))
                .toList();

        return new PedidoEvento(
                UUID.randomUUID().toString(),
                "PEDIDO_CREADO",
                pedido.getId(),
                pedido.getCliente(),
                pedido.getEmail(),
                pedido.getTotal(),
                items,
                (simulacion == null || simulacion.isBlank())
                        ? PedidoEvento.SIM_NINGUNA : simulacion,
                pedido.getFechaCreacion());
    }
}
