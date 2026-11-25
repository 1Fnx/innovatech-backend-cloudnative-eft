package cl.innovatech.notificaciones.service;

import cl.innovatech.notificaciones.exception.ErrorNoRecuperableException;
import cl.innovatech.notificaciones.exception.ErrorRecuperableException;
import cl.innovatech.notificaciones.messaging.evento.PedidoEvento;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Dominio funcional DOCUMENTOS: genera la boleta asociada al pedido.
 *
 * Es un dominio distinto al de notificaciones y por eso tiene su propia cola,
 * su propia DLQ y su propio consumidor, aunque reaccione al mismo evento.
 */
@Service
public class DocumentoService {

    private static final Logger log = LoggerFactory.getLogger(DocumentoService.class);

    private final RegistroProcesoService bitacora;

    public DocumentoService(RegistroProcesoService bitacora) {
        this.bitacora = bitacora;
    }

    public void generarBoleta(PedidoEvento evento) {

        if (evento.items() == null || evento.items().isEmpty()) {
            throw new ErrorNoRecuperableException(
                    "El pedido " + evento.pedidoId() + " no tiene lineas de detalle; no se puede emitir boleta");
        }

        if (PedidoEvento.SIM_ERROR_RECUPERABLE.equals(evento.simulacion())) {
            throw new ErrorRecuperableException(
                    "El servicio de folios electronicos no responde (fallo transitorio simulado)");
        }
        if (PedidoEvento.SIM_ERROR_NO_RECUPERABLE.equals(evento.simulacion())) {
            throw new ErrorNoRecuperableException(
                    "El pedido no cumple el formato tributario exigido (fallo permanente simulado)");
        }

        String folio = "BOL-" + String.format("%06d", evento.pedidoId());
        log.info("[DOCUMENTOS] Boleta {} generada para el pedido {} ({} lineas, total ${})",
                folio, evento.pedidoId(), evento.items().size(), evento.total());

        bitacora.registrar("BOLETA", evento.pedidoId(), evento.cliente(), "OK",
                "Boleta " + folio + " emitida con " + evento.items().size() + " linea(s)");
    }
}
