package com.juniverse.webclient.exception;

/** El servicio remoto respondió 404: el producto no existe. */
public class ProductNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Crea la excepción.
     *
     * @param id producto buscado
     */
    public ProductNotFoundException(long id) {
        super("Producto no encontrado: " + id);
    }
}
