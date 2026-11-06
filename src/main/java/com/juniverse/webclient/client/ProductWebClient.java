package com.juniverse.webclient.client;

import com.juniverse.webclient.exception.ProductNotFoundException;
import com.juniverse.webclient.exception.RemoteServiceException;
import com.juniverse.webclient.dto.CatalogSummary;
import com.juniverse.webclient.dto.NewProduct;
import com.juniverse.webclient.model.Product;
import io.netty.channel.ChannelOption;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.util.retry.Retry;

/**
 * Cliente reactivo con {@link WebClient}: no bloquea ningún hilo mientras espera la respuesta. Cada método devuelve un
 * {@link Mono} (0 o 1 valor) o un {@link Flux} (0..n valores) y <b>no hace nada hasta que alguien se suscribe</b>.
 *
 * <p>Muestra: URL base y cabeceras por defecto, tiempos de espera de Reactor Netty, filtros (logging), traducción de
 * errores con {@code onStatus}, {@code exchangeToMono}, reintentos con espera exponencial, llamadas en paralelo
 * ({@code flatMap}, {@code Mono.zip}) y streaming de eventos (SSE).</p>
 */
@Component
public class ProductWebClient {

    private static final Logger LOG = LoggerFactory.getLogger(ProductWebClient.class);

    private final WebClient client;

    /**
     * Crea el cliente con la URL base y los tiempos de espera configurados.
     *
     * @param baseUrl URL base del servicio de productos
     * @param connectTimeoutMs tiempo máximo para conectar
     * @param responseTimeoutMs tiempo máximo para recibir la respuesta
     */
    public ProductWebClient(@Value("${app.products.base-url}") String baseUrl,
            @Value("${app.products.connect-timeout-ms:1000}") int connectTimeoutMs,
            @Value("${app.products.response-timeout-ms:500}") long responseTimeoutMs) {
        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
                .responseTimeout(Duration.ofMillis(responseTimeoutMs));
        this.client = WebClient.builder()
                .baseUrl(baseUrl)
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .defaultHeader("X-Client", "webclient")
                .filter(logRequests())
                .build();
    }

    /** Filtro que registra cada petición y el código de respuesta (se aplica a todas las llamadas). */
    private static ExchangeFilterFunction logRequests() {
        return (request, next) -> {
            LOG.info("--> {} {}", request.method(), request.url());
            return next.exchange(request).doOnNext(response -> LOG.info("<-- {} {}", response.statusCode().value(), request.url()));
        };
    }

    /**
     * Lista los productos.
     *
     * @return flujo de productos
     */
    public Flux<Product> findAll() {
        return client.get().uri("/products")
                .retrieve()
                .onStatus(status -> status.isError(), response -> Mono.error(new RemoteServiceException("HTTP " + response.statusCode().value(), response.statusCode().value(), null)))
                .bodyToFlux(Product.class)
                .onErrorMap(WebClientRequestException.class, e -> noResponse(e));
    }

    /**
     * Busca un producto; un 404 se convierte en {@link ProductNotFoundException}.
     *
     * @param id identificador
     * @return el producto
     */
    public Mono<Product> findById(long id) {
        return client.get().uri("/products/{id}", id)
                .retrieve()
                .onStatus(status -> status.value() == HttpStatus.NOT_FOUND.value(), response -> Mono.error(new ProductNotFoundException(id)))
                .onStatus(status -> status.isError(), response -> Mono.error(new RemoteServiceException("HTTP " + response.statusCode().value(), response.statusCode().value(), null)))
                .bodyToMono(Product.class)
                .onErrorMap(WebClientRequestException.class, e -> noResponse(e));
    }

    /**
     * Igual que {@link #findById(long)} pero devuelve un Mono vacío si no existe, usando {@code exchangeToMono}, que da
     * acceso a la respuesta completa (código, cabeceras) para decidir qué hacer con ella.
     *
     * @param id identificador
     * @return el producto, o vacío si el servicio responde 404
     */
    public Mono<Product> findByIdOrEmpty(long id) {
        return client.get().uri("/products/{id}", id).exchangeToMono(response -> {
            if (response.statusCode().value() == HttpStatus.NOT_FOUND.value()) {
                return response.releaseBody().then(Mono.empty()); // libera la conexión y devuelve "nada"
            }
            if (response.statusCode().isError()) {
                return response.createException().flatMap(Mono::error);
            }
            return response.bodyToMono(Product.class);
        });
    }

    /**
     * Crea un producto.
     *
     * @param product datos
     * @return el producto creado
     */
    public Mono<Product> create(NewProduct product) {
        return client.post().uri("/products")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(product)
                .retrieve()
                .onStatus(status -> status.isError(), response -> Mono.error(new RemoteServiceException("HTTP " + response.statusCode().value(), response.statusCode().value(), null)))
                .bodyToMono(Product.class)
                .onErrorMap(WebClientRequestException.class, e -> noResponse(e));
    }

    /**
     * Elimina un producto.
     *
     * @param id identificador
     * @return Mono vacío que se completa al terminar
     */
    public Mono<Void> delete(long id) {
        return client.delete().uri("/products/{id}", id)
                .retrieve()
                .onStatus(status -> status.value() == HttpStatus.NOT_FOUND.value(), response -> Mono.error(new ProductNotFoundException(id)))
                .onStatus(status -> status.isError(), response -> Mono.error(new RemoteServiceException("HTTP " + response.statusCode().value(), response.statusCode().value(), null)))
                .bodyToMono(Void.class);
    }

    /**
     * Llama al endpoint lento. Con un tiempo de respuesta menor al retraso solicitado, falla por timeout.
     *
     * @param delayMs retraso que pide al proveedor
     * @return la respuesta del proveedor
     */
    public Mono<String> slow(long delayMs) {
        return client.get().uri(uri -> uri.path("/slow").queryParam("delayMs", delayMs).build())
                .retrieve()
                .bodyToMono(String.class)
                .onErrorMap(WebClientRequestException.class, e -> noResponse(e))
                .onErrorMap(TimeoutException.class, e -> new RemoteServiceException("tiempo de espera agotado", 0, e));
    }

    /**
     * Llama a un endpoint inestable reintentando los errores 5xx y los fallos de red hasta 3 veces, con espera
     * exponencial (100 ms, 200 ms, 400 ms...). Los 4xx NO se reintentan: repetirlos no los arreglaría.
     *
     * @param key clave de la serie de llamadas
     * @param failures cuántas veces falla el proveedor antes de funcionar
     * @return la respuesta del proveedor cuando por fin funciona
     */
    public Mono<String> flaky(String key, int failures) {
        return client.get().uri(uri -> uri.path("/flaky").queryParam("key", key).queryParam("failures", failures).build())
                .retrieve()
                .onStatus(status -> status.isError(), response -> Mono.error(new RemoteServiceException("HTTP " + response.statusCode().value(), response.statusCode().value(), null)))
                .bodyToMono(String.class)
                .retryWhen(Retry.backoff(3, Duration.ofMillis(100))
                        .filter(e -> e instanceof RemoteServiceException r && (r.getStatus() >= 500 || r.getStatus() == 0))
                        .onRetryExhaustedThrow((spec, signal) -> signal.failure())); // propaga el error original, no uno genérico
    }

    /**
     * Busca varios productos A LA VEZ (hasta 3 en paralelo) en lugar de uno tras otro.
     *
     * @param ids identificadores
     * @return productos, en el orden en que van llegando
     */
    public Flux<Product> findMany(List<Long> ids) {
        return Flux.fromIterable(ids).flatMap(this::findById, 3);
    }

    /**
     * Arma un resumen con dos llamadas independientes que se ejecutan simultáneamente ({@code Mono.zip}).
     *
     * @return resumen del catálogo
     */
    public Mono<CatalogSummary> summary() {
        Mono<Long> total = findAll().count();
        Mono<String> first = findAll().next().map(Product::name).defaultIfEmpty("(sin productos)");
        return Mono.zip(total, first).map(t -> new CatalogSummary(t.getT1().intValue(), t.getT2()));
    }

    /**
     * Consume un flujo de eventos del servidor (Server-Sent Events): cada evento llega apenas se emite, sin esperar a que
     * termine toda la respuesta.
     *
     * @return flujo de eventos de texto
     */
    public Flux<String> events() {
        return client.get().uri("/stream").accept(MediaType.TEXT_EVENT_STREAM).retrieve().bodyToFlux(String.class);
    }

    private static RemoteServiceException noResponse(WebClientRequestException e) {
        return new RemoteServiceException("sin respuesta (" + e.getMessage() + ")", 0, e);
    }
}
