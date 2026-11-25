package cl.innovatech.productos.service;

import cl.innovatech.productos.domain.Producto;
import cl.innovatech.productos.dto.ProductoRequest;
import cl.innovatech.productos.repository.ProductoRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/** Logica de negocio del catalogo de productos. */
@Service
public class ProductoService {

    private final ProductoRepository repositorio;

    public ProductoService(ProductoRepository repositorio) {
        this.repositorio = repositorio;
    }

    @Transactional(readOnly = true)
    public List<Producto> listar() { return repositorio.findAll(); }

    @Transactional(readOnly = true)
    public Producto obtener(Long id) { return repositorio.findById(id).orElse(null); }

    @Transactional
    public Producto crear(ProductoRequest datos) {
        return repositorio.save(new Producto(datos.nombre(), datos.descripcion(),
                datos.precio(), datos.stock()));
    }

    @Transactional
    public Producto actualizar(Long id, ProductoRequest datos) {
        return repositorio.findById(id).map(p -> {
            p.setNombre(datos.nombre());
            p.setDescripcion(datos.descripcion());
            p.setPrecio(datos.precio());
            p.setStock(datos.stock());
            return repositorio.save(p);
        }).orElse(null);
    }

    @Transactional
    public boolean eliminar(Long id) {
        if (!repositorio.existsById(id)) return false;
        repositorio.deleteById(id);
        return true;
    }
}
