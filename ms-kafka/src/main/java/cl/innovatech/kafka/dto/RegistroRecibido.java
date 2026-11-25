package cl.innovatech.kafka.dto;

/**
 * Registro tal como lo vio el consumidor.
 *
 * Es la version consultable por HTTP de lo que el @KafkaListener imprime en la
 * consola. Permite revisar desde el frontend la misma informacion que se
 * observa en la terminal: en que particion llego cada registro, con que offset
 * y con que clave.
 *
 * @param secuencia   numero correlativo de recepcion dentro de esta ejecucion;
 *                    es un contador local, no tiene relacion con el offset
 * @param topic       topic de origen
 * @param particion   particion desde la que se leyo
 * @param offset      posicion del registro dentro de esa particion
 * @param key         clave del registro, o null si se publico sin clave
 * @param valor       contenido del registro
 * @param grupo       grupo consumidor que realizo la lectura
 * @param recibidoEn  momento en que la aplicacion proceso el registro
 */
public record RegistroRecibido(
        long secuencia,
        String topic,
        int particion,
        long offset,
        String key,
        String valor,
        String grupo,
        String recibidoEn) {
}
