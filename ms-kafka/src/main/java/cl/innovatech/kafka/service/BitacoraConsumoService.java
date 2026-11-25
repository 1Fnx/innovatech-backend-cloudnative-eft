package cl.innovatech.kafka.service;

import cl.innovatech.kafka.config.KafkaProperties;
import cl.innovatech.kafka.dto.RegistroRecibido;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Conserva en memoria los ultimos registros que leyo el consumidor.
 *
 * Su unico proposito es poder mostrar en el frontend lo mismo que el
 * @KafkaListener imprime en la consola. No es un almacen de mensajes ni
 * sustituye al log de Kafka: el registro autoritativo vive en el broker, que ya
 * lo conserva de manera ordenada y persistente en cada particion. Esta clase
 * solo mantiene una ventana de los mas recientes.
 *
 * Por eso tampoco crece sin limite: al superar el maximo configurado descarta
 * el registro mas antiguo. Un acumulador sin tope en memoria seria una fuga
 * esperando ocurrir en un proceso de larga vida.
 *
 * Los metodos estan sincronizados porque el contenedor de escucha puede usar
 * mas de un hilo consumidor y la lista se lee desde los hilos que atienden
 * peticiones HTTP.
 */
@Service
public class BitacoraConsumoService {

    private final Deque<RegistroRecibido> registros = new ArrayDeque<>();
    private final AtomicLong contador = new AtomicLong();
    private final int maximo;

    public BitacoraConsumoService(KafkaProperties propiedades) {
        this.maximo = propiedades.getBitacoraMaxima();
    }

    /** Agrega un registro recien consumido y devuelve su numero de secuencia. */
    public synchronized long registrar(String topic, int particion, long offset,
                                       String key, String valor, String grupo,
                                       String recibidoEn) {
        long secuencia = contador.incrementAndGet();
        registros.addFirst(new RegistroRecibido(
                secuencia, topic, particion, offset, key, valor, grupo, recibidoEn));
        while (registros.size() > maximo) {
            registros.removeLast();
        }
        return secuencia;
    }

    /** Devuelve los registros observados, del mas reciente al mas antiguo. */
    public synchronized List<RegistroRecibido> listar() {
        return new ArrayList<>(registros);
    }

    /**
     * Vacia la ventana en memoria.
     *
     * No borra nada en Kafka ni mueve los offsets del grupo: solo limpia la
     * vista, de modo que una prueba nueva se observe sin el ruido de la
     * anterior.
     */
    public synchronized int limpiar() {
        int cantidad = registros.size();
        registros.clear();
        return cantidad;
    }
}
