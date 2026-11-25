package cl.innovatech.productos.repository;

import cl.innovatech.productos.domain.Producto;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductoRepository extends JpaRepository<Producto, Long> { }
