package cl.innovatech.kafka.dto;

import java.util.List;

/**
 * Descripcion del topic, equivalente por HTTP a la salida del comando
 * kafka-topics.sh --describe.
 *
 * @param topic       nombre del topic consultado
 * @param particiones cantidad total de particiones
 * @param replicas    copias de cada particion
 * @param detalle     una linea por particion, con su lider y sus replicas
 */
public record DescripcionTopic(
        String topic,
        int particiones,
        int replicas,
        List<ParticionDetalle> detalle) {

    /**
     * Estado de una particion concreta.
     *
     * @param particion      identificador de la particion, desde 0
     * @param lider          id del broker que atiende las lecturas y escrituras
     * @param replicas       ids de los brokers que guardan una copia
     * @param ultimoOffset   siguiente posicion que se escribira en esta particion,
     *                       es decir el avance actual del log
     */
    public record ParticionDetalle(
            int particion,
            int lider,
            List<Integer> replicas,
            long ultimoOffset) {
    }
}
