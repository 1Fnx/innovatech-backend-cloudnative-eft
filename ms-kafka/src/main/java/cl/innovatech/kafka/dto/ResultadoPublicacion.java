package cl.innovatech.kafka.dto;

/**
 * Respuesta del endpoint de publicacion.
 *
 * Reune los metadatos que devuelve el broker tras aceptar el registro. El dato
 * central son particion y offset: son asignados por Kafka, no por la
 * aplicacion, y permiten comprobar como se reparten los mensajes.
 *
 * Importante al interpretarla: que este objeto llegue con exito significa que
 * el broker escribio el registro en el log. NO significa que un consumidor ya
 * lo haya procesado. Son dos hechos distintos y ocurren en momentos distintos.
 *
 * @param mensajeLegible resumen en una linea, en el mismo formato que muestra
 *                       la consola, para poder leerlo directo desde curl
 * @param topic          topic de destino
 * @param particion      particion asignada por el particionador
 * @param offset         posicion que ocupo el registro dentro de esa particion
 * @param key            clave enviada, o null si se publico sin clave
 * @param valor          contenido del registro
 * @param instante       marca de tiempo del registro en el log
 */
public record ResultadoPublicacion(
        String mensajeLegible,
        String topic,
        int particion,
        long offset,
        String key,
        String valor,
        String instante) {
}
