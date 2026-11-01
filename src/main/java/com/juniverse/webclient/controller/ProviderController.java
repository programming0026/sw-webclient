package com.juniverse.webclient.controller;

import com.juniverse.webclient.model.NewProduct;
import com.juniverse.webclient.model.Product;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Servicio "proveedor" reactivo (la API externa que se va a consumir): productos en memoria, un endpoint lento,
 * uno que falla las primeras veces y uno que emite eventos en streaming (Server-Sent Events).
 */
@RestController
@RequestMapping("/provider")
public class ProviderController {

    private final Map<Long, Product> products = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong(3);

    /** Carga tres productos de ejemplo. */
    public ProviderController() {
        products.put(1L, new Product(1L, "Teclado mecánico", new BigDecimal("899.00")));
        products.put(2L, new Product(2L, "Ratón inalámbrico", new BigDecimal("349.50")));
        products.put(3L, new Product(3L, "Monitor 27\"", new BigDecimal("4599.90")));
    }

    /**
     * Lista los productos ordenados por id.
     *
     * @return flujo de productos
     */
    @GetMapping("/products")
    public Flux<Product> list() {
        return Flux.fromStream(() -> products.values().stream().sorted((a, b) -> Long.compare(a.id(), b.id())));
    }

    /**
     * Obtiene un producto.
     *
     * @param id identificador
     * @return 200 con el producto o 404
     */
    @GetMapping("/products/{id}")
    public Mono<ResponseEntity<Product>> get(@PathVariable long id) {
        return Mono.justOrEmpty(products.get(id)).map(ResponseEntity::ok).defaultIfEmpty(ResponseEntity.notFound().build());
    }

    /**
     * Crea un producto.
     *
     * @param request datos
     * @return 201 con el producto o 400 si los datos no son válidos
     */
    @PostMapping("/products")
    public Mono<ResponseEntity<Product>> create(@RequestBody NewProduct request) {
        if (request.name() == null || request.name().isBlank() || request.price() == null || request.price().signum() < 0) {
            return Mono.just(ResponseEntity.badRequest().build());
        }
        Product product = new Product(sequence.incrementAndGet(), request.name(), request.price());
        products.put(product.id(), product);
        return Mono.just(ResponseEntity.status(HttpStatus.CREATED).body(product));
    }

    /**
     * Elimina un producto.
     *
     * @param id identificador
     * @return 204 o 404
     */
    @DeleteMapping("/products/{id}")
    public Mono<ResponseEntity<Void>> delete(@PathVariable long id) {
        return Mono.just(products.remove(id) == null ? ResponseEntity.notFound().build() : ResponseEntity.noContent().build());
    }

    /**
     * Endpoint lento: responde tras esperar el tiempo indicado (para provocar un timeout en el cliente).
     *
     * @param delayMs milisegundos de espera
     * @return texto fijo
     */
    @GetMapping("/slow")
    public Mono<String> slow(@RequestParam(defaultValue = "1000") long delayMs) {
        return Mono.just("respuesta lenta").delayElement(Duration.ofMillis(delayMs));
    }

    /**
     * Endpoint "inestable": falla con 503 las primeras {@code failures} veces que se llama con la misma clave
     * y después responde bien. Sirve para probar los reintentos.
     *
     * @param key clave que identifica la serie de llamadas
     * @param failures cuántas veces debe fallar antes de funcionar
     * @return 503 mientras falla; después 200 con el número de intentos
     */
    @GetMapping("/flaky")
    public Mono<ResponseEntity<String>> flaky(@RequestParam String key, @RequestParam(defaultValue = "2") int failures) {
        int attempt = attempts.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
        if (attempt <= failures) {
            return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("falló el intento " + attempt));
        }
        return Mono.just(ResponseEntity.ok("ok tras " + attempt + " intentos"));
    }

    /**
     * Emite cinco eventos, uno cada 100 ms, como Server-Sent Events.
     *
     * @return flujo de eventos de texto
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> stream() {
        return Flux.interval(Duration.ofMillis(100)).take(5).map(i -> "evento-" + (i + 1));
    }
}
