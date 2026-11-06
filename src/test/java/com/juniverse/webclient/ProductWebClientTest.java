package com.juniverse.webclient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.juniverse.webclient.client.ProductWebClient;
import com.juniverse.webclient.exception.ProductNotFoundException;
import com.juniverse.webclient.exception.RemoteServiceException;
import com.juniverse.webclient.dto.CatalogSummary;
import com.juniverse.webclient.dto.NewProduct;
import com.juniverse.webclient.model.Product;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.test.StepVerifier;

/**
 * Pruebas del cliente reactivo con {@code StepVerifier} (reactor-test): describe qué señales debe emitir el flujo
 * (valores, error o finalización). La aplicación arranca en un puerto libre y las llamadas son HTTP reales.
 */
// Límite de respuesta de 1500 ms en las pruebas (500 ms en la aplicación): da margen a la primera llamada en frío
// en máquinas cargadas, como las de CI, y sigue siendo menor que los 2000 ms del endpoint lento.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
        properties = "app.products.response-timeout-ms=1500")
@DisplayName("ProductWebClient con StepVerifier")
class ProductWebClientTest {

    private static final int PORT = freePort();

    @DynamicPropertySource
    static void ports(DynamicPropertyRegistry registry) {
        registry.add("server.port", () -> PORT);
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Autowired
    private ProductWebClient client;

    @Test
    @DisplayName("findAll emite los productos y se completa")
    void findAll() {
        StepVerifier.create(client.findAll().map(Product::name))
                .expectNext("Teclado mecánico", "Ratón inalámbrico", "Monitor 27\"")
                .thenConsumeWhile(name -> true)
                .verifyComplete();
    }

    @Test
    @DisplayName("findById emite el producto")
    void findById() {
        StepVerifier.create(client.findById(2))
                .assertNext(p -> {
                    assertEquals("Ratón inalámbrico", p.name());
                    assertEquals(0, new BigDecimal("349.50").compareTo(p.price()));
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("un 404 termina con ProductNotFoundException")
    void notFound() {
        StepVerifier.create(client.findById(9999)).expectError(ProductNotFoundException.class).verify();
    }

    @Test
    @DisplayName("exchangeToMono: un 404 devuelve un Mono vacío, sin error")
    void findByIdOrEmpty() {
        StepVerifier.create(client.findByIdOrEmpty(9999)).verifyComplete();
        StepVerifier.create(client.findByIdOrEmpty(1).map(Product::id)).expectNext(1L).verifyComplete();
    }

    @Test
    @DisplayName("crear, buscar y eliminar")
    void createFindDelete() {
        Product created = client.create(new NewProduct("Webcam", new BigDecimal("799.00"))).block(Duration.ofSeconds(2));
        assertTrue(created != null && created.id() != null);

        StepVerifier.create(client.findById(created.id()).map(Product::name)).expectNext("Webcam").verifyComplete();
        StepVerifier.create(client.delete(created.id())).verifyComplete();
        StepVerifier.create(client.findById(created.id())).expectError(ProductNotFoundException.class).verify();
    }

    @Test
    @DisplayName("datos inválidos: RemoteServiceException con el 400 del servidor")
    void badRequest() {
        StepVerifier.create(client.create(new NewProduct(" ", new BigDecimal("1"))))
                .expectErrorSatisfies(e -> {
                    assertTrue(e instanceof RemoteServiceException);
                    assertEquals(400, ((RemoteServiceException) e).getStatus());
                })
                .verify();
    }

    @Test
    @DisplayName("timeout: la respuesta tarda más que el límite de respuesta")
    void timeout() {
        StepVerifier.create(client.slow(2000))
                .expectErrorSatisfies(e -> assertEquals(0, ((RemoteServiceException) e).getStatus()))
                .verify(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("una respuesta rápida no agota el tiempo de espera")
    void fastResponse() {
        StepVerifier.create(client.slow(50)).expectNext("respuesta lenta").verifyComplete();
    }

    @Test
    @DisplayName("reintentos: falla dos veces con 503 y a la tercera funciona")
    void retryUntilItWorks() {
        String key = UUID.randomUUID().toString();

        StepVerifier.create(client.flaky(key, 2)).expectNext("ok tras 3 intentos").verifyComplete();
    }

    @Test
    @DisplayName("reintentos agotados: si siempre falla, se propaga el error original (503)")
    void retriesExhausted() {
        String key = UUID.randomUUID().toString();

        StepVerifier.create(client.flaky(key, 100))
                .expectErrorSatisfies(e -> assertEquals(503, ((RemoteServiceException) e).getStatus()))
                .verify(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("findMany: varias llamadas en paralelo con flatMap")
    void findMany() {
        StepVerifier.create(client.findMany(List.of(1L, 2L, 3L)).map(Product::id).collectList())
                .assertNext(ids -> assertEquals(List.of(1L, 2L, 3L), ids.stream().sorted().toList(), "llegan en cualquier orden"))
                .verifyComplete();
    }

    @Test
    @DisplayName("summary: combina dos llamadas con Mono.zip")
    void summary() {
        StepVerifier.create(client.summary())
                .assertNext((CatalogSummary s) -> {
                    assertTrue(s.totalProducts() >= 3);
                    assertEquals("Teclado mecánico", s.firstProductName());
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("streaming SSE: llegan los 5 eventos en orden")
    void serverSentEvents() {
        StepVerifier.create(client.events())
                .expectNext("evento-1", "evento-2", "evento-3", "evento-4", "evento-5")
                .verifyComplete();
    }
}
