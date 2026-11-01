# Consumir servicios REST con WebClient (Spring WebFlux)

Ejemplo con **Spring Boot 4**, **Spring WebFlux** y **Java 21** que consume una API REST de forma **reactiva y no bloqueante** con `WebClient`.

Para que funcione sin internet, la propia aplicación publica un servicio "proveedor" en `/provider` (también reactivo) y el cliente lo consume por **HTTP real**. Cambiando `app.products.base-url` puedes apuntarlo a otra API con las mismas rutas.

## ¿Por qué WebClient?

`RestTemplate` y `RestClient` **bloquean** el hilo mientras esperan la respuesta. `WebClient` no: devuelve un `Mono` (0 o 1 valor) o un `Flux` (0..n valores) y libera el hilo mientras llega la respuesta. Con pocos hilos se pueden atender muchísimas llamadas simultáneas, y se pueden combinar con operadores (`flatMap`, `zip`, `retryWhen`, `timeout`...).

> Regla clave: un `Mono`/`Flux` **no hace nada hasta que alguien se suscribe**. En un controlador de WebFlux, el framework se suscribe por ti; en el resto del código usa `subscribe()`, `block()` (solo en pruebas o `main`) o `StepVerifier`.

## Qué muestra el ejemplo (`ProductWebClient`)

| Técnica | Método |
|---------|--------|
| URL base, cabecera por defecto y **filtro** de logging (`ExchangeFilterFunction`) | constructor |
| Tiempos de espera de Reactor Netty (conexión y respuesta) | constructor |
| Lista como `Flux` | `findAll()` |
| `retrieve()` + **`onStatus`**: un 404 se convierte en `ProductNotFoundException` y otros errores en `RemoteServiceException` | `findById()`, `delete()`, `create()` |
| **`exchangeToMono`**: acceso a la respuesta completa; un 404 devuelve un `Mono` vacío en lugar de error | `findByIdOrEmpty()` |
| Envío de JSON con `bodyValue` | `create()` |
| **Timeout** de la respuesta | `slow()` |
| **Reintentos con espera exponencial** (`retryWhen(Retry.backoff...)`), solo para 5xx y fallos de red | `flaky()` |
| **Llamadas en paralelo** con `flatMap(..., concurrencia)` | `findMany()` |
| Combinar dos llamadas simultáneas con **`Mono.zip`** | `summary()` |
| **Streaming** de eventos del servidor (SSE) con `bodyToFlux` | `events()` |

## Estructura

```text
src/main/java/com/juniverse/webclient/
├── WebClientApplication.java
├── client/ProductWebClient.java       # el cliente reactivo (todo lo de la tabla anterior)
├── controller/
│   ├── ProviderController.java        # el servicio "externo": productos, /slow, /flaky y /stream (SSE)
│   └── ConsumerController.java        # API propia /consume/... (devuelve Mono/Flux)
├── exception/                         # ProductNotFoundException, RemoteServiceException
└── model/                             # Product, NewProduct, CatalogSummary
```

## Requisitos

- JDK 21 y Maven 3.9 o superior.

## Ejecutar

```bash
mvn spring-boot:run
```

Y en otra terminal:

```bash
curl http://localhost:8080/consume/products
curl http://localhost:8080/consume/products/2
curl http://localhost:8080/consume/products/999                         # 404
curl "http://localhost:8080/consume/many?ids=1,2,3"                      # tres llamadas en paralelo
curl http://localhost:8080/consume/summary                              # dos llamadas combinadas con zip
curl "http://localhost:8080/consume/flaky?key=prueba1&failures=2"       # falla 2 veces y se reintenta: "ok tras 3 intentos"
curl -N http://localhost:8080/consume/events                            # eventos llegando uno a uno (SSE)
curl -X POST http://localhost:8080/consume/products -H "Content-Type: application/json" \
     -d '{"name":"Webcam","price":799.00}'
```

## Pruebas

```bash
mvn test
```

`ProductWebClientTest` usa **`StepVerifier`** (reactor-test) para describir qué señales debe emitir cada flujo: valores, error o finalización. Cubre 13 casos: listar, buscar, 404, `exchangeToMono`, crear/buscar/borrar, 400, timeout, respuesta rápida, reintentos que funcionan y reintentos agotados, llamadas en paralelo, `zip` y SSE. La aplicación arranca en un puerto libre y las llamadas son HTTP reales.

## Buenas prácticas con WebClient

- **Nunca llames a `block()` dentro de código reactivo** (bloquearía un hilo del event loop). Solo en `main`, pruebas o código tradicional.
- Reutiliza **una instancia** de `WebClient` (se construye una vez); es segura entre hilos.
- Define siempre **tiempos de espera** y decide qué errores se reintentan (nunca los 4xx).
- Libera el cuerpo cuando uses `exchangeToMono` (`releaseBody()` o `bodyToMono`), o la conexión quedará colgada.
- Para limitar la carga sobre el servicio remoto usa la concurrencia de `flatMap`.

Fat jar ejecutable: `mvn clean package` y `java -jar target/sw-webclient-0.0.1-SNAPSHOT.jar`. Mira también `sbj-consume-services` (RestTemplate, RestClient y Feign, que son bloqueantes).

## Licencia

Código de ejemplo de uso libre con fines educativos.
