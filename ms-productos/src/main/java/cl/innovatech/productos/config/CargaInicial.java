package cl.innovatech.productos.config;

import cl.innovatech.productos.domain.Producto;
import cl.innovatech.productos.repository.ProductoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Catalogo inicial del comercio.
 *
 * Se ejecuta en cualquier perfil, no solo contra la base de datos en memoria:
 * una instalacion nueva contra una base de datos administrada tambien necesita
 * un catalogo con el que trabajar. La carga es idempotente, porque se detiene
 * en cuanto encuentra productos cargados, de modo que reiniciar el servicio no
 * duplica nada ni pisa lo que el usuario haya creado despues.
 *
 * Se desactiva con app.catalogo.carga-inicial en false.
 */
@Configuration
@ConditionalOnProperty(name = "app.catalogo.carga-inicial", havingValue = "true", matchIfMissing = true)
public class CargaInicial {

    private static final Logger log = LoggerFactory.getLogger(CargaInicial.class);

    @Bean
    public CommandLineRunner cargarProductos(ProductoRepository repositorio) {
        return args -> {
            long existentes = repositorio.count();
            if (existentes > 0) {
                log.info("[CATALOGO] Ya hay {} productos cargados. No se inserta nada.", existentes);
                return;
            }

            repositorio.save(new Producto("Alimento Premium Adulto 15kg",
                    "Croquetas balanceadas para perro adulto", 32990.0, 25));
            repositorio.save(new Producto("Alimento Cachorro 10kg",
                    "Nutricion completa para cachorros", 27990.0, 18));
            repositorio.save(new Producto("Snacks Dentales",
                    "Snacks para la higiene dental canina", 4990.0, 60));
            repositorio.save(new Producto("Cama Ortopedica Mediana",
                    "Cama con espuma viscoelastica", 45990.0, 8));
            repositorio.save(new Producto("Juguete Mordedor Resistente",
                    "Juguete de caucho natural", 7990.0, 32));

            log.info("[CATALOGO] Catalogo inicial cargado: {} productos.", repositorio.count());
        };
    }
}
