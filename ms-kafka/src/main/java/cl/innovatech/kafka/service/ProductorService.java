package cl.innovatech.kafka.service;

import cl.innovatech.kafka.config.KafkaProperties;
import cl.innovatech.kafka.dto.ResultadoPublicacion;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * Encapsula el lado productor.
 *
 * Es la unica clase del microservicio que conoce KafkaTemplate. El controlador
 * recibe la peticion HTTP y delega aqui, igual que en el trabajo con RabbitMQ
 * el servicio de negocio no tocaba RabbitTemplate directamente. Si manana el
 * transporte cambiara, solo habria que reescribir esta clase.
 *
 * Sobre la espera de confirmacion: KafkaTemplate.send devuelve un futuro y
 * publica de manera asincrona. Aqui se llama a get() con un tiempo limite para
 * obtener los metadatos que el broker asigno, que son justamente lo que se
 * quiere observar en el laboratorio: particion y offset. En una aplicacion con
 * carga real no conviene bloquear el hilo que atiende la peticion HTTP de esta
 * forma; se usaria el callback asincrono del futuro.
 */
@Service
public class ProductorService {

    private static final Logger log = LoggerFactory.getLogger(ProductorService.class);

    private static final DateTimeFormatter FORMATO =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final KafkaTemplate<String, String> plantilla;
    private final KafkaProperties propiedades;

    /** Inyeccion por constructor: deja explicitas las dependencias de la clase. */
    public ProductorService(KafkaTemplate<String, String> plantilla,
                            KafkaProperties propiedades) {
        this.plantilla = plantilla;
        this.propiedades = propiedades;
    }

    /**
     * Publica un registro en el topic configurado y devuelve los metadatos
     * asignados por el broker.
     *
     * El valor de la clave determina el comportamiento del particionador:
     *   - Clave nula: el particionador por defecto decide la particion sin
     *     consultar ninguna clave. Puede agrupar varios registros consecutivos
     *     en la misma particion, por lo que no se debe esperar el reparto
     *     0, 1, 2 de forma obligatoria.
     *   - Clave presente: la particion se calcula a partir del hash de la
     *     clave. Todos los registros con la misma clave caen en la misma
     *     particion mientras no cambie el numero de particiones ni el
     *     particionador, lo que conserva su orden relativo en el log.
     *
     * @param mensaje contenido del registro
     * @param clave   clave opcional; una cadena vacia se trata como ausencia
     */
    public ResultadoPublicacion publicar(String mensaje, String clave) throws Exception {
        String topic = propiedades.getTopic();
        // Una clave vacia enviada desde un formulario equivale a no enviar clave.
        String claveEfectiva = (clave == null || clave.isBlank()) ? null : clave.trim();

        log.debug("[PRODUCTOR] Publicando. topic={} key={} valor={}", topic, claveEfectiva, mensaje);

        SendResult<String, String> resultado = plantilla
                .send(topic, claveEfectiva, mensaje)
                .get(propiedades.getEsperaConfirmacionSegundos(), TimeUnit.SECONDS);

        RecordMetadata metadatos = resultado.getRecordMetadata();

        String instante = LocalDateTime
                .ofInstant(Instant.ofEpochMilli(metadatos.timestamp()), ZoneId.systemDefault())
                .format(FORMATO);

        // Resumen en una linea, con el mismo formato que se imprime en consola:
        // asi la respuesta de curl y el log del consumidor se pueden comparar
        // dato por dato dentro de una misma respuesta.
        String legible = String.format(
                "Publicado: particion=%d, offset=%d, key=%s, message=%s",
                metadatos.partition(), metadatos.offset(), claveEfectiva, mensaje);

        log.info("[PRODUCTOR] {}", legible);

        return new ResultadoPublicacion(
                legible,
                topic,
                metadatos.partition(),
                metadatos.offset(),
                claveEfectiva,
                mensaje,
                instante);
    }
}
