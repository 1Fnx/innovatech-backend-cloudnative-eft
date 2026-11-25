package cl.innovatech.adminrabbit.dto;

/** Estado actual de una cola consultada en el broker. */
public record ColaInfoResponse(String nombre, int mensajes, int consumidores) { }
