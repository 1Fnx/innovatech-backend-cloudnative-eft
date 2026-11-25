package cl.innovatech.productos.messaging.listener;

import org.springframework.amqp.core.Message;

import java.util.List;
import java.util.Map;

/**
 * Utilidad que interpreta el encabezado "x-death" que RabbitMQ agrega a todo
 * mensaje que llega a una Dead Letter Queue.
 *
 * Ese encabezado explica POR QUE y DESDE DONDE murio el mensaje, y es la
 * informacion que se deja registrada en el log para su posterior diagnostico:
 *   reason = rejected | expired | maxlen | delivery_limit
 */
public final class MetadatosDlq {

    private MetadatosDlq() { }

    public record Resumen(String razon, String colaOrigen, String exchangeOrigen,
                          String routingKey, Long conteo) {
        @Override
        public String toString() {
            return "razon=" + razon + " colaOrigen=" + colaOrigen
                    + " exchangeOrigen=" + exchangeOrigen
                    + " routingKey=" + routingKey + " veces=" + conteo;
        }
    }

    @SuppressWarnings("unchecked")
    public static Resumen leer(Message mensaje) {
        Map<String, Object> headers = mensaje.getMessageProperties().getHeaders();
        Object bruto = headers.get("x-death");

        if (!(bruto instanceof List<?> lista) || lista.isEmpty()) {
            return new Resumen("desconocida", "desconocida", "desconocido", "desconocida", 0L);
        }
        Object primero = lista.get(0);
        if (!(primero instanceof Map)) {
            return new Resumen("desconocida", "desconocida", "desconocido", "desconocida", 0L);
        }
        Map<String, Object> death = (Map<String, Object>) primero;

        String razon = String.valueOf(death.getOrDefault("reason", "desconocida"));
        String cola = String.valueOf(death.getOrDefault("queue", "desconocida"));
        String exchange = String.valueOf(death.getOrDefault("exchange", "desconocido"));
        Object rks = death.get("routing-keys");
        String routingKey = (rks instanceof List<?> l && !l.isEmpty())
                ? String.valueOf(l.get(0)) : "desconocida";
        Object count = death.get("count");
        Long veces = (count instanceof Number n) ? n.longValue() : 0L;

        return new Resumen(razon, cola, exchange, routingKey, veces);
    }
}
