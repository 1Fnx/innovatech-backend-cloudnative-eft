package cl.innovatech.kafka.dto;

import java.util.List;

/**
 * Respuesta comun a las operaciones de administracion.
 *
 * @param operacion CREAR, AMPLIAR, CONFIGURAR o ELIMINAR
 * @param detalle   texto listo para mostrarse al usuario
 * @param aviso     advertencia asociada a la operacion, o null si no la hay
 */
public record ResultadoAdministracion(
        String operacion,
        List<String> afectados,
        String detalle,
        String aviso) {

    public static ResultadoAdministracion de(String operacion, List<String> afectados, String detalle) {
        return new ResultadoAdministracion(operacion, afectados, detalle, null);
    }
}
