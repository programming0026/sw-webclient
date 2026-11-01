package com.juniverse.webclient.model;

/**
 * Resumen que se arma con DOS llamadas simultáneas ({@code Mono.zip}).
 *
 * @param totalProducts cantidad de productos del catálogo
 * @param firstProductName nombre del primer producto
 */
public record CatalogSummary(int totalProducts, String firstProductName) {
}
