package cl.innovatech.notificaciones.service;

import cl.innovatech.notificaciones.exception.ErrorNoRecuperableException;
import cl.innovatech.notificaciones.exception.ErrorRecuperableException;
import cl.innovatech.notificaciones.messaging.evento.PedidoEvento;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Dominio funcional NOTIFICACIONES: avisa al cliente que su pedido fue recibido.
 *
 * Logica de negocio pura: no conoce canales, colas ni confirmaciones AMQP. Se
 * limita a hacer el trabajo o a lanzar una excepcion que distingue un fallo
 * transitorio de uno definitivo, y es el listener quien traduce esa distincion
 * en ACK, reintento o envio a la DLQ.
 */
@Service
public class NotificacionService {

    private static final Logger log = LoggerFactory.getLogger(NotificacionService.class);

    private final RegistroProcesoService bitacora;

    public NotificacionService(RegistroProcesoService bitacora) {
        this.bitacora = bitacora;
    }

    public void enviarNotificacion(PedidoEvento evento) {

        // Validacion de negocio: sin correo el aviso jamas podra enviarse.
        if (evento.email() == null || evento.email().isBlank()) {
            throw new ErrorNoRecuperableException(
                    "El pedido " + evento.pedidoId() + " no trae correo de destino");
        }

        // Puntos de inyeccion de fallo para ejercitar las rutas de error.
        if (PedidoEvento.SIM_ERROR_RECUPERABLE.equals(evento.simulacion())) {
            throw new ErrorRecuperableException(
                    "El proveedor de correo no responde (fallo transitorio simulado)");
        }
        if (PedidoEvento.SIM_ERROR_NO_RECUPERABLE.equals(evento.simulacion())) {
            throw new ErrorNoRecuperableException(
                    "La plantilla de correo del pedido es invalida (fallo permanente simulado)");
        }

        String texto = "Hola " + evento.cliente() + ", recibimos tu pedido N°"
                + evento.pedidoId() + " por $" + evento.total() + ". Gracias por comprar en Innovatech.";

        log.info("[NOTIFICACIONES] Correo enviado a {} -> {}", evento.email(), texto);
        bitacora.registrar("NOTIFICACION", evento.pedidoId(), evento.cliente(), "OK",
                "Aviso enviado a " + evento.email());
    }
}
