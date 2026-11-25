package cl.innovatech.kafka.dto;

import java.util.List;

/**
 * Estado del grupo consumidor, equivalente por HTTP a la salida del comando
 * kafka-consumer-groups.sh --describe --group.
 *
 * @param grupo    identificador del grupo consultado
 * @param estado   situacion informada por el broker (Stable, Empty, etc.)
 * @param miembros cantidad de consumidores activos en el grupo
 * @param avance   una fila por particion, con offset confirmado, avance del
 *                 log y el retraso resultante
 */
public record EstadoGrupo(
        String grupo,
        String estado,
        int miembros,
        List<AvanceParticion> avance) {

    /**
     * Posicion del grupo en una particion.
     *
     * @param particion      particion observada
     * @param offsetActual   ultima posicion confirmada por el grupo; es lo que
     *                       el grupo retomaria tras un reinicio. Equivale a la
     *                       columna CURRENT-OFFSET
     * @param offsetFinalLog avance del log en esa particion. Equivale a la
     *                       columna LOG-END-OFFSET
     * @param retraso        diferencia entre ambos, es decir cuanto le falta
     *                       leer al grupo. Equivale a la columna LAG
     */
    public record AvanceParticion(
            int particion,
            long offsetActual,
            long offsetFinalLog,
            long retraso) {
    }
}
