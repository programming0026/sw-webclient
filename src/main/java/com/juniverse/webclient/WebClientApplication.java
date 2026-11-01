package com.juniverse.webclient;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Consumo de servicios REST de forma <b>reactiva y no bloqueante</b> con {@code WebClient} (Spring WebFlux, Java 21).
 *
 * <p>Para que el ejemplo funcione sin internet, la misma aplicación publica un servicio "proveedor" en {@code /provider}
 * y el cliente lo consume por HTTP real. Cambiando {@code app.products.base-url} se puede apuntar a otra API.</p>
 */
@SpringBootApplication
public class WebClientApplication {

    /**
     * Arranca la aplicación.
     *
     * @param args argumentos de la línea de comandos
     */
    public static void main(String[] args) {
        SpringApplication.run(WebClientApplication.class, args);
    }
}
