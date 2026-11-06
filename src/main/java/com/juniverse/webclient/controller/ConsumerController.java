package com.juniverse.webclient.controller;

import com.juniverse.webclient.client.ProductWebClient;
import com.juniverse.webclient.exception.ProductNotFoundException;
import com.juniverse.webclient.exception.RemoteServiceException;
import com.juniverse.webclient.dto.CatalogSummary;
import com.juniverse.webclient.dto.NewProduct;
import com.juniverse.webclient.model.Product;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * API propia reactiva que usa el {@code WebClient}. Los controladores devuelven {@code Mono}/{@code Flux} sin bloquear:
 * Spring WebFlux se suscribe y escribe la respuesta cuando llegan los datos.
 */
@RestController
@RequestMapping("/consume")
public class ConsumerController {

    private final ProductWebClient client;

    /**
     * Crea el controlador.
     *
     * @param client cliente reactivo de productos
     */
    public ConsumerController(ProductWebClient client) {
        this.client = client;
    }

    /**
     * Lista los productos.
     *
     * @return productos
     */
    @GetMapping("/products")
    public Flux<Product> list() {
        return client.findAll();
    }

    /**
     * Obtiene un producto.
     *
     * @param id identificador
     * @return el producto
     */
    @GetMapping("/products/{id}")
    public Mono<Product> get(@PathVariable long id) {
        return client.findById(id);
    }

    /**
     * Crea un producto.
     *
     * @param product datos
     * @return el producto creado
     */
    @PostMapping("/products")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Product> create(@RequestBody NewProduct product) {
        return client.create(product);
    }

    /**
     * Elimina un producto.
     *
     * @param id identificador
     * @return 204
     */
    @DeleteMapping("/products/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> delete(@PathVariable long id) {
        return client.delete(id);
    }

    /**
     * Busca varios productos en paralelo.
     *
     * @param ids identificadores separados por coma
     * @return productos encontrados
     */
    @GetMapping("/many")
    public Flux<Product> many(@RequestParam List<Long> ids) {
        return client.findMany(ids);
    }

    /**
     * Resumen del catálogo con dos llamadas simultáneas.
     *
     * @return resumen
     */
    @GetMapping("/summary")
    public Mono<CatalogSummary> summary() {
        return client.summary();
    }

    /**
     * Reenvía en streaming los eventos del proveedor.
     *
     * @return eventos SSE
     */
    @GetMapping(value = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> events() {
        return client.events();
    }

    /**
     * Llama al endpoint inestable con reintentos.
     *
     * @param key clave de la serie de llamadas
     * @param failures cuántas veces falla antes de funcionar
     * @return respuesta del proveedor
     */
    @GetMapping("/flaky")
    public Mono<String> flaky(@RequestParam String key, @RequestParam(defaultValue = "2") int failures) {
        return client.flaky(key, failures);
    }

    /**
     * 404: el producto no existe.
     *
     * @param e excepción
     * @return respuesta de error
     */
    @ExceptionHandler(ProductNotFoundException.class)
    public ResponseEntity<Map<String, String>> notFound(ProductNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", e.getMessage()));
    }

    /**
     * 502 si el servicio remoto falla o no responde; los 4xx se propagan.
     *
     * @param e excepción
     * @return respuesta de error
     */
    @ExceptionHandler(RemoteServiceException.class)
    public ResponseEntity<Map<String, String>> remote(RemoteServiceException e) {
        int status = e.getStatus() >= 400 && e.getStatus() < 500 ? e.getStatus() : HttpStatus.BAD_GATEWAY.value();
        return ResponseEntity.status(status).body(Map.of("error", e.getMessage()));
    }
}
