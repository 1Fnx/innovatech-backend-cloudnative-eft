package cl.innovatech.notificaciones.domain;

import jakarta.persistence.*;

/**
 * Bitacora de cada mensaje procesado. Permite demostrar, desde el frontend o
 * desde la API, que el trabajo asincrono efectivamente ocurrio y con que
 * resultado (procesado correctamente, reintentado o enviado a la DLQ).
 */
@Entity
@Table(name = "registro_proceso")
public class RegistroProceso {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Dominio funcional: NOTIFICACION, BOLETA o DLQ. */
    @Column(nullable = false, length = 30)
    private String dominio;

    @Column(name = "pedido_id")
    private Long pedidoId;

    @Column(length = 120)
    private String cliente;

    /** OK, REINTENTO o DLQ. */
    @Column(nullable = false, length = 20)
    private String resultado;

    @Column(length = 500)
    private String detalle;

    @Column(nullable = false, length = 40)
    private String fecha;

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getDominio() { return dominio; }
    public void setDominio(String dominio) { this.dominio = dominio; }
    public Long getPedidoId() { return pedidoId; }
    public void setPedidoId(Long pedidoId) { this.pedidoId = pedidoId; }
    public String getCliente() { return cliente; }
    public void setCliente(String cliente) { this.cliente = cliente; }
    public String getResultado() { return resultado; }
    public void setResultado(String resultado) { this.resultado = resultado; }
    public String getDetalle() { return detalle; }
    public void setDetalle(String detalle) { this.detalle = detalle; }
    public String getFecha() { return fecha; }
    public void setFecha(String fecha) { this.fecha = fecha; }
}
