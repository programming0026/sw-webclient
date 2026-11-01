package com.juniverse.webclient.exception;

/**
 * Fallo al llamar al servicio remoto: error 4xx/5xx distinto de 404, servicio caído o tiempo de espera agotado.
 */
public class RemoteServiceException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int status;

    /**
     * Crea la excepción.
     *
     * @param message descripción
     * @param status código HTTP recibido, o 0 si no hubo respuesta (red caída o timeout)
     * @param cause causa original
     */
    public RemoteServiceException(String message, int status, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    /**
     * Código HTTP recibido.
     *
     * @return código, o 0 si no hubo respuesta
     */
    public int getStatus() {
        return status;
    }
}
