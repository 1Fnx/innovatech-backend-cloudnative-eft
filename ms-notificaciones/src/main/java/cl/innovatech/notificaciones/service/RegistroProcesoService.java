package cl.innovatech.notificaciones.service;

import cl.innovatech.notificaciones.domain.RegistroProceso;
import cl.innovatech.notificaciones.repository.RegistroProcesoRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/** Guarda y consulta la bitacora de procesamiento de mensajes. */
@Service
public class RegistroProcesoService {

    private static final DateTimeFormatter FORMATO = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final RegistroProcesoRepository repositorio;

    public RegistroProcesoService(RegistroProcesoRepository repositorio) {
        this.repositorio = repositorio;
    }

    public void registrar(String dominio, Long pedidoId, String cliente,
                          String resultado, String detalle) {
        RegistroProceso registro = new RegistroProceso();
        registro.setDominio(dominio);
        registro.setPedidoId(pedidoId);
        registro.setCliente(cliente);
        registro.setResultado(resultado);
        registro.setDetalle(detalle != null && detalle.length() > 500
                ? detalle.substring(0, 500) : detalle);
        registro.setFecha(LocalDateTime.now().format(FORMATO));
        repositorio.save(registro);
    }

    public List<RegistroProceso> ultimos() {
        return repositorio.findTop50ByOrderByIdDesc();
    }
}
