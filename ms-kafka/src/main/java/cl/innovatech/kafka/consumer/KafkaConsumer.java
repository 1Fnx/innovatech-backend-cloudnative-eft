package cl.innovatech.kafka.consumer;

import cl.innovatech.kafka.config.KafkaProperties;
import cl.innovatech.kafka.service.BitacoraConsumoService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Lado consumidor del laboratorio.
 *
 * La anotacion @KafkaListener suscribe este metodo al topic indicado. Spring
 * levanta por detras un contenedor de escucha que mantiene abierta la conexion
 * con el broker, pide registros de forma continua e invoca este metodo por cada
 * uno que llega.
 *
 * El topic y el grupo no se escriben como literales: se resuelven con
 * placeholders contra las claves app.kafka.topic y app.kafka.grupo-id del
 * archivo application.yml. Como spring.kafka.consumer.group-id apunta a esa
 * misma clave, el grupo queda definido en un solo lugar. Esto importa para la
 * prueba de relectura: cambiar el grupo exige editar un unico valor, y no se
 * corre el riesgo de dejar la anotacion y el archivo con nombres distintos.
 *
 * Se recibe el ConsumerRecord completo en lugar de extraer los metadatos con
 * anotaciones @Header sueltas. Asi se obtienen clave, valor, particion y offset
 * del mismo objeto, y una clave nula no provoca ningun problema al resolver los
 * argumentos del metodo.
 */
@Service
public class KafkaConsumer {

    private static final DateTimeFormatter FORMATO =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final BitacoraConsumoService bitacora;
    private final KafkaProperties propiedades;

    public KafkaConsumer(BitacoraConsumoService bitacora, KafkaProperties propiedades) {
        this.bitacora = bitacora;
        this.propiedades = propiedades;
    }

    /**
     * Procesa un registro leido del topic.
     *
     * Los cuatro datos que interesan observar:
     *   particion : en cual de los logs del topic quedo escrito el registro.
     *   offset    : su posicion dentro de esa particion. Cada particion lleva
     *               su propia numeracion, por lo que dos particiones pueden
     *               mostrar el mismo numero de offset sin que exista relacion
     *               alguna entre ambos registros.
     *   key       : la clave con que se publico, o ausente si no se envio.
     *   valor     : el contenido.
     *
     * Que este metodo termine sin excepcion significa que la aplicacion
     * proceso el registro. Es un hecho distinto de la respuesta HTTP que
     * recibio quien publico: esa solo confirmaba que el broker lo habia
     * escrito en el log.
     */
    @KafkaListener(
            topics = "${app.kafka.topic}",
            groupId = "${app.kafka.grupo-id}")
    public void escuchar(ConsumerRecord<String, String> registro) {

        String momento = LocalDateTime.now().format(FORMATO);
        String clave = registro.key();

        // Se conserva el registro en memoria para poder consultarlo despues por
        // HTTP y mostrarlo en la pantalla del frontend.
        long secuencia = bitacora.registrar(
                registro.topic(),
                registro.partition(),
                registro.offset(),
                clave,
                registro.value(),
                propiedades.getGrupoId(),
                momento);

        // Bloque visual para la consola. Se imprime primero el detalle linea a
        // linea, por legibilidad, y al final el mismo dato en una sola linea,
        // que es el formato comodo para compararlo contra la respuesta del
        // endpoint de publicacion.
        System.out.println("=".repeat(96));
        System.out.printf(" MENSAJE RECIBIDO  (n %d - %s)%n", secuencia, momento);
        System.out.printf("   Topic     : %s%n", registro.topic());
        System.out.printf("   Particion : %d%n", registro.partition());
        System.out.printf("   Offset    : %d%n", registro.offset());
        System.out.printf("   Key       : %s%n", clave != null ? clave : "(sin key)");
        System.out.printf("   Mensaje   : %s%n", registro.value());
        System.out.printf("   Grupo     : %s%n", propiedades.getGrupoId());
        System.out.printf("RECIBIDO | particion=%d | offset=%d | key=%s | valor=%s%n",
                registro.partition(), registro.offset(), clave, registro.value());
        System.out.println("=".repeat(96));
    }
}
