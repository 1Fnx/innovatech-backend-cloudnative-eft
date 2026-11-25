package cl.innovatech.pedidos.repository;

import cl.innovatech.pedidos.domain.Pedido;
import org.springframework.data.jpa.repository.JpaRepository;

/** Repositorio JPA del agregado Pedido. */
public interface PedidoRepository extends JpaRepository<Pedido, Long> { }
