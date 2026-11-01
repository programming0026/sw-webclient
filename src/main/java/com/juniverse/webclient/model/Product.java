package com.juniverse.webclient.model;

import java.math.BigDecimal;

/**
 * Producto devuelto por el servicio.
 *
 * @param id identificador
 * @param name nombre
 * @param price precio
 */
public record Product(Long id, String name, BigDecimal price) {
}
