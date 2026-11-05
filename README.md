# BikeStore Async v1.0

Procesamiento asíncrono de pedidos con **Java 17 + RabbitMQ**.

```
Front-end ──POST /orders──▶ OrderProducer ──JSON──▶ [bikestore.orders] ──▶ payment.queue ──▶ PaymentWorker
                                                                                              │
     ┌─ fallo y quedan reintentos ─▶ payment.retry.queue ──(TTL 2 s)──▶ vuelve a [bikestore.orders]
     ├─ fallo tras 3 reintentos ───▶ [bikestore.orders.dlx] ──▶ payment.dlq (análisis offline)
     └─ resultado PAID / FAILED ───▶ [bikestore.payments] ──▶ email.queue ──▶ EmailWorker (envía solo si PAID)
```

## Requisitos

- Java 17 o superior, Maven 3.8+
- Docker (para RabbitMQ)

## 1. Levantar RabbitMQ

```bash
docker compose up -d
```

Consola web: http://localhost:15672 (usuario `bikestore`, clave `bikestore`).

## 2. Opción A: todo en una JVM (rápido para la demo)

```bash
mvn -q compile exec:java -Dexec.args="demo"
```

Arranca PaymentWorker (2 consumidores), EmailWorker y OrderProducer, y envía 5 pedidos de prueba.

## 2. Opción B: cada componente en su terminal (muestra la independencia)

```bash
mvn -q package
java -cp target/bikestore-async.jar com.bikestore.worker.PaymentWorker 2   # terminal 1
java -cp target/bikestore-async.jar com.bikestore.worker.EmailWorker       # terminal 2
java -cp target/bikestore-async.jar com.bikestore.producer.OrderProducer   # terminal 3
```

Enviar un pedido (terminal 4):

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"cliente":"Ana Pérez","email":"ana@example.com","items":[{"producto":"Bicicleta MTB","cantidad":1,"precioUnitario":689.90}]}'
# -> {"pedidoId":"PED-3f9a1c2e","status":"ACCEPTED"}
```

En Windows PowerShell:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/orders -ContentType "application/json" `
  -Body '{"cliente":"Ana","email":"ana@example.com","items":[{"producto":"Casco","cantidad":1,"precioUnitario":59.99}]}'
```

## 3. Revisar la Dead-Letter Queue

```bash
java -cp target/bikestore-async.jar com.bikestore.worker.DlqInspector
```

Lista los pedidos que agotaron los reintentos **sin borrarlos** de `payment.dlq`.
También se ven en la consola web: Queues → `payment.dlq` → Get messages.

## Salida esperada (ejemplo)

Cada línea lleva `timestamp`, `[thread]`, componente y `pedidoId`:

```
2026-11-04T15:02:11.101Z [http-1]           OrderProducer  pedidoId=PED-3f9a1c2e | Pedido recibido por API: cliente=Ana Pérez, items=1, total=$689.90
2026-11-04T15:02:11.118Z [http-1]           OrderProducer  pedidoId=PED-3f9a1c2e | Mensaje JSON publicado en bikestore.orders [order.created] y confirmado por el bróker
2026-11-04T15:02:11.125Z [payment-worker-1] PaymentWorker  pedidoId=PED-3f9a1c2e | Procesando cobro: intento 1/4, monto=$689.90
2026-11-04T15:02:11.512Z [payment-worker-1] PaymentWorker  pedidoId=PED-3f9a1c2e | Cobro RECHAZADO -> reintento 1/3 programado en 2000 ms
2026-11-04T15:02:13.530Z [payment-worker-2] PaymentWorker  pedidoId=PED-3f9a1c2e | Procesando cobro: intento 2/4, monto=$689.90
2026-11-04T15:02:13.901Z [payment-worker-2] PaymentWorker  pedidoId=PED-3f9a1c2e | Cobro APROBADO -> evento paymentStatus=PAID publicado en bikestore.payments
2026-11-04T15:02:13.910Z [email-worker-1]   EmailWorker    pedidoId=PED-3f9a1c2e | Evento de pago recibido: paymentStatus=PAID
2026-11-04T15:02:14.062Z [email-worker-1]   EmailWorker    pedidoId=PED-3f9a1c2e | E-mail de confirmación ENVIADO a ana@example.com (total $689.90)
```

Con 50 % de fallo, la probabilidad de que un pedido falle los 4 intentos es 0.5^4 ≈ 6 %;
para ver la DLQ rápido: `PAYMENT_FAIL_RATE=0.9 mvn -q compile exec:java -Dexec.args="demo"`.

## Variables de entorno

| Variable | Default | Uso |
| --- | --- | --- |
| `RABBITMQ_HOST` / `RABBITMQ_PORT` | localhost / 5672 | Bróker |
| `RABBITMQ_USER` / `RABBITMQ_PASS` | bikestore / bikestore | Credenciales |
| `HTTP_PORT` | 8080 | API del OrderProducer |
| `PAYMENT_FAIL_RATE` | 0.5 | Probabilidad de fallo del cobro |
| `MAX_RETRIES` | 3 | Reintentos antes de la DLQ |
| `RETRY_DELAY_MS` | 2000 | Espera entre reintentos |

> Si cambias `RETRY_DELAY_MS`, borra antes `payment.retry.queue` en la consola: RabbitMQ no
> permite redeclarar una cola con argumentos distintos.

## Estructura

```
src/main/java/com/bikestore/
├── App.java                     # arranca todo + modo demo
├── common/
│   ├── Config.java              # nombres de colas/exchanges y parámetros
│   ├── Log.java                 # log con timestamp, thread y pedidoId
│   ├── OrderItem.java
│   ├── OrderMessage.java        # contrato JSON del mensaje
│   └── Rabbit.java              # conexión y declaración de la topología
├── producer/OrderProducer.java  # API POST /orders -> publica en el bróker
└── worker/
    ├── PaymentWorker.java       # cobro simulado, reintentos y DLQ
    ├── EmailWorker.java         # e-mail solo si PAID
    └── DlqInspector.java        # análisis offline de la DLQ
```

## Decisiones de diseño

- **Reintentos con espera sin bloquear la cola**: el mensaje fallido se republica en `payment.retry.queue`
  (TTL 2 s) y RabbitMQ lo devuelve solo a `payment.queue`. Así un pedido con problemas no frena a los demás.
- **DLQ nativa**: tras el tercer reintento el worker hace `basicReject(requeue=false)` y el
  dead-letter exchange lo deja en `payment.dlq` con el header `x-death` (motivo y cola de origen).
- **Coreografía**: el PaymentWorker publica el resultado del cobro; el EmailWorker reacciona solo a `PAID`.
- **At-least-once**: ack manual, mensajes persistentes y publisher confirms antes de confirmar el original.
  En producción el cobro debería ser idempotente por `pedidoId` para tolerar duplicados.
