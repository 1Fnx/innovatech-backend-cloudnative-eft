package cl.innovatech.notificaciones.repository;

import cl.innovatech.notificaciones.domain.RegistroProceso;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface RegistroProcesoRepository extends JpaRepository<RegistroProceso, Long> {
    List<RegistroProceso> findTop50ByOrderByIdDesc();
}
