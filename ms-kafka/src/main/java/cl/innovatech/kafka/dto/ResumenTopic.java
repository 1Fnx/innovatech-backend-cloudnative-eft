package cl.innovatech.kafka.dto;

import java.util.List;
import java.util.Map;

/**
 * Ficha de un topico: lo que por consola entregan kafka-topics --describe y
 * kafka-configs --describe juntos.
 *
 * @param esDlt         el nombre termina en el sufijo de descartes
 * @param configuracion valores fijados en el topico; no incluye los heredados
 *                      del broker
 */
public record ResumenTopic(
        String nombre,
        int particiones,
        int replicas,
        boolean interno,
        boolean esDlt,
        List<ParticionResumen> detalle,
        Map<String, String> configuracion) {

    /**
     * @param lider            broker que atiende la particion; -1 si no hay
     *                         lider y la particion no esta disponible
     * @param replicasEnSincro copias al dia con el lider. Si su cantidad cae
     *                         bajo min.insync.replicas, el topico deja de
     *                         aceptar escrituras acks=all
     */
    public record ParticionResumen(
            int particion,
            int lider,
            List<Integer> replicas,
            List<Integer> replicasEnSincro) {
    }
}
