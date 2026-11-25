package cl.innovatech.kafka.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.Arrays;

/**
 * Autoriza al frontend a invocar esta API desde el navegador.
 *
 * El origen permitido se declara en application.yml y no se usa un comodin,
 * para no abrir la API a cualquier sitio web.
 *
 * Conviene recordar el reparto de responsabilidades: CORS es una restriccion
 * que aplica el navegador sobre la capa HTTP. Kafka no participa en ella. El
 * recorrido real es React -> HTTP -> Spring Boot -> protocolo Kafka -> broker;
 * el navegador nunca abre una conexion contra el puerto 9092.
 */
@Configuration
public class CorsConfig implements WebMvcConfigurer {

    @Value("${app.cors.allowed-origins}")
    private String origenesPermitidos;

    @Override
    public void addCorsMappings(CorsRegistry registro) {
        registro.addMapping("/api/**")
                .allowedOrigins(Arrays.stream(origenesPermitidos.split(","))
                        .map(String::trim)
                        .toArray(String[]::new))
                .allowedMethods("GET", "POST", "PATCH", "DELETE", "OPTIONS")
                .allowedHeaders("Content-Type", "Authorization")
                .maxAge(3600);
    }
}
