package cl.innovatech.kafka.dto;

import java.util.List;

/**
 * Estado agregado de un grupo consumidor.
 *
 * EstadoGrupo entrega el detalle por particion de un solo grupo; este registro
 * permite listar todos los del cluster de una sola consulta.
 *
 * @param estado       Stable con miembros activos, Empty si ninguno esta conectado
 * @param retrasoTotal suma del retraso de todas sus particiones
 */
public record ResumenGrupo(
        String grupo,
        String estado,
        int miembros,
        List<String> topics,
        long retrasoTotal) {
}
