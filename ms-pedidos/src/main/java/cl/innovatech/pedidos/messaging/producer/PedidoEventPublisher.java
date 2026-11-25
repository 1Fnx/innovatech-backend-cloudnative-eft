package cl.innovatech.pedidos.messaging.producer;

import cl.innovatech.pedidos.messaging.evento.PedidoEvento;

/**
 * Puerto de salida de mensajeria.
 *
 * El servicio de negocio depende de esta interfaz y no de RabbitTemplate.
 * Sustituir la tecnologia de mensajeria implica escribir otra implementacion
 * de este puerto, sin tocar la capa de negocio.
 */
public interface PedidoEventPublisher {

    /** Publica el evento de "pedido creado" en el exchange correspondiente. */
    void publicarPedidoCreado(PedidoEvento evento);
}
