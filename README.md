# Innovatech Chile - Backend

Plataforma de microservicios de la tienda de alimento para perros, construida
por etapas y sin reemplazar nada de lo anterior:

| Etapa | Que se agrego                                                        |
|-------|----------------------------------------------------------------------|
| EP1   | API REST y seguridad con **Microsoft Entra External ID** (OAuth2 / OIDC) |
| EP2   | Mensajeria por colas con **RabbitMQ**: exchanges, DLX / DLQ y cluster |
| EP3   | Streaming de eventos con **Apache Kafka**: ZooKeeper, tres brokers, dos topics y un pipeline de dos etapas |

Las tres capas **conviven**. Kafka no reemplaza a RabbitMQ: cada tecnologia
atiende el caso de uso que le corresponde, y la comparacion esta en la
seccion 13.3.

---

## 1. Arquitectura

```
                        (HTTP + JWT)
   Frontend React ───────────────────────┐
                                         │
                                         ▼
                              ┌────────────────────┐
                              │ ms-pedidos  :8082  │  PRODUCTOR
                              │ POST /api/pedidos  │
                              └─────────┬──────────┘
                                        │ publica 1 evento
                                        │ routing key: pedido.creado
                                        ▼
                    ┌───────────────────────────────────────┐
                    │ innovatech.pedidos.exchange  (topic)  │
                    └───┬───────────────┬───────────────┬───┘
                        │               │               │
       pedidos.notificaciones.queue  pedidos.documentos.queue  pedidos.inventario.queue
                        │               │               │
                        ▼               ▼               ▼
            ms-notificaciones:8083  ms-notificaciones:8083  ms-productos:8081
            NotificacionListener    DocumentoListener       InventarioListener
                        │               │               │
                        └──── basicNack(requeue=false) ──┘
                                        │
                                        ▼
                              ┌────────────────────┐
                              │ innovatech.dlx     │  Dead Letter Exchange
                              └───┬──────┬─────────┘
                                  ▼      ▼       ▼
          pedidos.notificaciones.dlq  pedidos.documentos.dlq  pedidos.inventario.dlq
                                  │      │       │
                                  └── listeners de auditoria ──┘

                              ┌────────────────────────┐
                              │ ms-admin-rabbit :8084  │  administra la topologia
                              │ /api/rabbit/**         │  via RabbitAdmin
                              └────────────────────────┘
```

| Microservicio       | Puerto | Rol                                                      |
|---------------------|--------|----------------------------------------------------------|
| `ms-productos`      | 8081   | Catalogo y stock. Consume la cola de **inventario**.     |
| `ms-pedidos`        | 8082   | **Productor**: guarda el pedido y publica el evento.     |
| `ms-notificaciones` | 8083   | Consume **notificaciones** y **documentos** + bitacora.  |
| `ms-admin-rabbit`   | 8084   | Administra colas, exchanges y bindings por REST.         |
| `ms-kafka`          | 8888   | **Administrador y observador** del cluster de Kafka: crea, modifica y elimina topics, e inspecciona grupos y offsets. Ver secciones 12 y 14. |

Con Kafka en el medio, el flujo del pedido se abre en dos canales
independientes (seccion 13.1):

```
   ms-pedidos ──┬── AMQP  ──▶ RabbitMQ (colas, EP2)
                └── Kafka ──▶ innovatech.pedidos.eventos
                                   │
                                   ├─▶ ms-productos    (grupo-inventario)
                                   │        └──▶ innovatech.inventario.movimientos
                                   │                    └─▶ ms-notificaciones (grupo-alertas)
                                   └─▶ ms-notificaciones (grupo-notificaciones)
```

---

## 2. Requisitos

- Java 17 (`java -version`)
- Maven 3.9 o superior (`mvn -version`)
- Docker Desktop corriendo (`docker --version`)

---

## 3. Levantar RabbitMQ

Desde esta carpeta:

```bash
docker compose up -d
docker compose ps
```

- AMQP (lo usan los microservicios): `localhost:5672`
- Consola de administracion: http://localhost:15672
- Usuario / clave: `admin` / `admin123`

Para apagarlo conservando los datos:

```bash
docker compose stop
```

Para apagarlo y borrar el volumen (deja el broker vacio):

```bash
docker compose down -v
```

### Cluster de dos nodos (opcional)

```bash
docker compose -f docker-compose.cluster.yml up -d
docker exec -it rabbitmq-1 rabbitmqctl cluster_status
```

Consolas: http://localhost:15672 (nodo 1) y http://localhost:15673 (nodo 2).

> El cluster y el broker simple usan el mismo puerto 5672, por lo que **no se
> levantan los dos a la vez**. Detener uno antes de iniciar el otro.

---

## 4. Compilar

```bash
mvn -f ms-productos/pom.xml clean package -DskipTests
mvn -f ms-pedidos/pom.xml clean package -DskipTests
mvn -f ms-notificaciones/pom.xml clean package -DskipTests
mvn -f ms-admin-rabbit/pom.xml clean package -DskipTests
mvn -f ms-kafka/pom.xml clean package -DskipTests
```

---

## 5. Levantar los microservicios

Cada uno en una **terminal distinta**, desde esta carpeta. El perfil `demo` usa
base de datos H2 en memoria y no necesita MySQL.

```bash
# Terminal 1
mvn -f ms-productos/pom.xml spring-boot:run -Dspring-boot.run.profiles=demo

# Terminal 2
mvn -f ms-pedidos/pom.xml spring-boot:run -Dspring-boot.run.profiles=demo

# Terminal 3
mvn -f ms-notificaciones/pom.xml spring-boot:run -Dspring-boot.run.profiles=demo

# Terminal 4
mvn -f ms-admin-rabbit/pom.xml spring-boot:run -Dspring-boot.run.profiles=demo

# Terminal 5 (laboratorio de Kafka; no usa base de datos, por eso sin perfil)
mvn -f ms-kafka/pom.xml spring-boot:run
```

Comprobacion rapida (endpoints publicos, no piden token):

```bash
curl http://localhost:8081/api/health
curl http://localhost:8082/api/health
curl http://localhost:8083/api/health
curl http://localhost:8084/api/health
curl http://localhost:8888/api/kafka/health
```

---

## 6. Como se declara la topologia

Toda la nomenclatura esta **centralizada** en `application.yml`, bajo
`app.rabbit`. Ninguna clase escribe el nombre de un exchange o de una cola como
texto fijo: lo lee desde `MensajeriaProperties`.

```yaml
app:
  rabbit:
    exchanges:
      pedidos: innovatech.pedidos.exchange   # topic
      alertas: innovatech.alertas.direct     # direct
      dlx: innovatech.dlx                    # topic
    routing-keys:
      pedido-creado: pedido.creado
      alerta-stock-bajo: alerta.stock.bajo
      dlq-notificaciones: dlq.notificaciones
      dlq-documentos: dlq.documentos
      dlq-inventario: dlq.inventario
      dlq-alertas: dlq.alertas
    queues:
      notificaciones: pedidos.notificaciones.queue
      documentos: pedidos.documentos.queue
      inventario: pedidos.inventario.queue
      alertas-stock: alertas.stock.queue
      notificaciones-dlq: pedidos.notificaciones.dlq
      documentos-dlq: pedidos.documentos.dlq
      inventario-dlq: pedidos.inventario.dlq
      alertas-stock-dlq: alertas.stock.dlq
    auditoria-dlq: true
    reintentos:
      max-intentos: 2
```

### 6.1 Los dos tipos de exchange

El sistema usa los dos tipos de enrutamiento porque resuelven problemas
distintos:

| Exchange | Tipo | Enrutamiento | Por que |
|---|---|---|---|
| `innovatech.pedidos.exchange` | `topic` | la clave se compara contra un patron | un mismo `pedido.creado` se copia a tres colas de dominio |
| `innovatech.alertas.direct` | `direct` | coincidencia exacta de la clave | la alerta de reposicion tiene un unico destinatario |
| `innovatech.dlx` | `topic` | patron por dominio | separa las fallas de cada flujo en su propia DLQ |

El recorrido de la alerta es: `ms-productos` descuenta el stock, evalua el
umbral de `app.inventario.umbral-stock-bajo` y, si alguna linea quedo por
debajo, publica en `innovatech.alertas.direct` con la clave
`alerta.stock.bajo`. `ms-notificaciones` la consume desde
`alertas.stock.queue` y la registra en la bitacora.

El mismo hecho viaja tambien por Kafka, en el topico de movimientos. No es una
duplicacion por descuido: la cola transporta **la tarea de reponer**, dirigida a
un responsable, y el topico transporta **el hecho de que el stock bajo**, que
cualquier interesado puede consumir. Es la misma distincion que separa a los dos
canales en todo el sistema.

### 6.2 Auditoria de las DLQ

Con `app.rabbit.auditoria-dlq` en `true`, los consumidores de auditoria leen
cada DLQ, dejan en el log el bloque de cuarentena con la cabecera `x-death`
(motivo del descarte y cola de origen) y confirman el mensaje. Ponerlo en
`false` deja los mensajes acumulados en la cola, lo que sirve para inspeccionar
su contenido desde la consola del broker.

Las colas se declaran con `RabbitAdmin` al arrancar, cada una apuntando a su
DLQ mediante los argumentos `x-dead-letter-exchange` y
`x-dead-letter-routing-key`.

---

## 7. Manejo de errores en los consumidores

Los listeners usan **ACK manual** (`acknowledge-mode: manual`, `prefetch: 1`),
por lo que cada mensaje se confirma o se rechaza de forma explicita:

| Situacion                                  | Accion                              | Destino final       |
|--------------------------------------------|-------------------------------------|---------------------|
| Procesado correctamente                    | `basicAck`                          | sale de la cola     |
| Error **no recuperable**                   | `basicNack(requeue = false)`        | DLQ de inmediato    |
| Error **recuperable**, primera entrega     | `basicNack(requeue = true)`         | vuelve a la cola    |
| Error **recuperable**, ya era reintento    | `basicNack(requeue = false)`        | DLQ                 |
| Excepcion inesperada                       | `basicNack(requeue = false)`        | DLQ                 |

El limite de un reintento evita el *poison message*: un mensaje defectuoso no
puede quedar rebotando para siempre entre la cola y el consumidor.

Los listeners de las DLQ leen la cabecera `x-death` para registrar **por que**
cayo el mensaje (`rejected`, `expired`, `maxlen`) y de que cola venia.

---

## 8. Probar los escenarios de error

El campo `simulacion` del pedido permite forzar cada camino:

```bash
# Procesamiento correcto
curl -X POST http://localhost:8082/api/pedidos \
  -H "Authorization: Bearer <TOKEN>" -H "Content-Type: application/json" \
  -d '{"cliente":"Ignacio","email":"ig@duocuc.cl","simulacion":"NINGUNA",
       "items":[{"productoId":1,"nombre":"Alimento Premium Adulto 15kg","cantidad":2,"precio":42990}]}'

# Error recuperable: reintenta una vez y luego va a la DLQ
# ... "simulacion":"ERROR_RECUPERABLE"

# Error no recuperable: va directo a la DLQ
# ... "simulacion":"ERROR_NO_RECUPERABLE"
```

Desde el frontend lo mismo se hace con el selector de la pantalla *Mensajeria*.

---

## 9. Endpoints de administracion (ms-admin-rabbit)

| Metodo | Ruta                                     | Autorizacion            |
|--------|------------------------------------------|-------------------------|
| GET    | `/api/health`                            | publico                 |
| GET    | `/api/rabbit/topologia`                  | scope `Productos.Read`  |
| POST   | `/api/rabbit/queues`                     | scope `Productos.Write` |
| GET    | `/api/rabbit/queues/{nombre}`            | scope `Productos.Read`  |
| POST   | `/api/rabbit/queues/{nombre}/purge`      | rol `Admin`             |
| DELETE | `/api/rabbit/queues/{nombre}`            | rol `Admin`             |
| POST   | `/api/rabbit/exchanges`                  | scope `Productos.Write` |
| DELETE | `/api/rabbit/exchanges/{nombre}`         | rol `Admin`             |
| POST   | `/api/rabbit/bindings`                   | scope `Productos.Write` |
| DELETE | `/api/rabbit/bindings`                   | rol `Admin`             |

Todas las entradas pasan por **Bean Validation**. Si llega un nombre vacio o un
tipo de exchange invalido, el `ManejadorDeErrores` responde `400` con el detalle
campo por campo, sin que la peticion llegue a tocar el broker.

---

## 10. Seguridad

Los cuatro microservicios validan el JWT emitido por Entra External ID:

- firma, contra las claves publicas del `jwk-set-uri`
- emisor (`issuer`)
- audiencia (`audience`), con un validador propio
- vigencia (`exp`)

Los `scp` del token se convierten en authorities `SCOPE_*` y los `roles` en
`ROLE_*`, lo que permite autorizar endpoint por endpoint con `@PreAuthorize`.

---

## 11. Perfiles

| Perfil  | Base de datos                   | Uso                       |
|---------|---------------------------------|---------------------------|
| `demo`  | H2 en memoria                   | pruebas locales (default) |
| `cloud` | MySQL (variables de entorno)    | base de datos administrada |

La integracion con la base de datos se resuelve con las tres piezas de JPA:
**entidades** (`Pedido`, `PedidoItem`, `Producto`, `RegistroProceso`),
**repositorios** (`PedidoRepository`, `ProductoRepository`,
`RegistroProcesoRepository`) y **propiedades de conexion** declaradas por perfil.
Cambiar de H2 a MySQL no exige tocar ninguna clase Java: basta activar el perfil
`cloud` y entregar `DB_URL`, `DB_USER` y `DB_PASS`.

```bash
# Local, sin instalar nada
mvn spring-boot:run -Dspring-boot.run.profiles=demo

# Contra la base de datos administrada
$env:DB_URL="jdbc:mysql://<host>:3306/innovatech"
$env:DB_USER="innovatech"
$env:DB_PASS="<clave>"
mvn spring-boot:run -Dspring-boot.run.profiles=cloud
```

`ms-kafka` y `ms-admin-rabbit` no usan base de datos, por lo que no dependen de
estos perfiles.

---

## 12. Apache Kafka: cluster y topologia (EP3)

Mensajeria por **log distribuido**, en contraste con el modelo de colas de
RabbitMQ. Donde RabbitMQ entrega un mensaje y lo retira de la cola, Kafka
**conserva** cada registro en una particion y cada grupo consumidor lleva su
propio marcador de lectura, el *offset*.

Las dos tecnologias **conviven**: no se reemplazo nada del EP2. Cada una atiende
el caso de uso que le corresponde, y la razon esta en la seccion 13.3.

### 12.1 El cluster

El archivo `docker-compose.kafka.yml` levanta **tres nodos ZooKeeper** y **tres
brokers**, mas la consola **Kafka UI**.

```
  ┌───────────────── ensemble ZooKeeper ─────────────────┐
  │  zookeeper-1:2181   zookeeper-2:2182   zookeeper-3:2183 │
  │  se comunican entre si por 2888 (sincronizacion)        │
  │  y 3888 (eleccion de lider)                             │
  └───────────────────────┬─────────────────────────────────┘
                          │ metadatos, lideres de particion
                          │ y eleccion del broker coordinador
  ┌───────────────────────▼─────────────────────────────────┐
  │   kafka-1:9092     kafka-2:9093     kafka-3:9094        │
  │   (hacia Windows)                                       │
  │   kafka-1:29092    kafka-2:29092    kafka-3:29092       │
  │   (entre contenedores)                                  │
  └───────────────────────┬─────────────────────────────────┘
                          │
                  ┌───────▼────────┐
                  │ Kafka UI :8080 │
                  └────────────────┘
```

**Por que tres ZooKeeper y no uno.** ZooKeeper decide por mayoria. Con tres
nodos la mayoria es dos, de modo que el ensemble sigue operando si cae uno. Con
un solo nodo no hay mayoria posible: su caida deja al cluster sin quien
coordine. Un ensemble de dos nodos es peor que uno de tres, porque la mayoria
sigue siendo dos y cualquier caida lo detiene.

**Por que dos listeners por broker.** Un cliente que corre en Windows debe
conectarse a `localhost:9092`, mientras que Kafka UI, que vive dentro de la red
de Docker, debe usar `kafka-1:29092`. El broker anuncia ambas direcciones y cada
cliente usa la que puede alcanzar. Si solo se anunciara el nombre interno,
Spring Boot resolveria `kafka-1` y no lo encontraria.

### 12.2 Los dos topics del negocio

| Topic                              | Particiones | Replicas | min.insync.replicas | Retencion |
|------------------------------------|-------------|----------|---------------------|-----------|
| `innovatech.pedidos.eventos`       | 3           | 3        | 2                   | 7 dias    |
| `innovatech.inventario.movimientos`| 3           | 3        | 2                   | 7 dias    |
| `innovatech.pedidos.eventos.DLT`       | 3       | 3        | 2                   | 7 dias    |
| `innovatech.inventario.movimientos.DLT`| 3       | 3        | 2                   | 7 dias    |
| `innovatech.laboratorio.particiones`   | 3       | 3        | 2                   | 7 dias    |

**Tres particiones.** Cada particion es un log ordenado e independiente. Tres
particiones permiten que hasta tres consumidores del mismo grupo trabajen en
paralelo: un grupo no puede tener mas consumidores activos que particiones. El
orden se garantiza *dentro* de cada particion, y como la clave del mensaje es el
identificador del pedido, todos los eventos de un mismo pedido caen en la misma
particion y conservan su orden relativo.

**Tres replicas.** Cada particion se guarda en los tres brokers. La caida de un
nodo no deja ninguna particion sin copias disponibles.

**min.insync.replicas = 2.** Combinado con `acks=all` en el productor, un
mensaje aceptado quedo escrito en al menos dos brokers distintos. El cluster
tolera la caida de un broker y sigue aceptando escrituras; con el valor en 1 se
aceptarian escrituras con una sola copia viva, lo que abre la puerta a perder
datos si ese nodo tambien cae.

**Retencion de 7 dias y cleanup.policy = delete.** En Kafka el mensaje **no** se
borra al ser leido: se borra cuando vence este plazo. Por eso un grupo
consumidor nuevo puede leer eventos antiguos, y por eso se puede reprocesar el
historial. `compact`, la alternativa, conserva solo el ultimo valor de cada
clave y sirve para topics que representan un estado, no un historial de hechos.

**La creacion automatica de topics esta desactivada**
(`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`). Es deliberado: con ella activada, un
error de tipeo en el nombre crearia un topic nuevo y vacio en lugar de dar
error, y el problema apareceria mucho despues, cuando nadie encuentra los
mensajes. Los topics se declaran en `KafkaTopologiaConfig` de `ms-kafka`, que es
el **unico** responsable de la topologia: los microservicios que publican y
consumen no crean infraestructura.

### 12.3 Levantar el cluster

```bash
docker compose -f docker-compose.kafka.yml config      # valida la sintaxis
docker compose -f docker-compose.kafka.yml up -d
docker compose -f docker-compose.kafka.yml ps
docker compose -f docker-compose.kafka.yml logs --tail=40 kafka-1
```

| Puerto        | Servicio                                                   |
|---------------|------------------------------------------------------------|
| 2181/2182/2183| ZooKeeper, un puerto de cliente por nodo                   |
| 9092/9093/9094| brokers para clientes en Windows (`localhost:909x`)        |
| 29092         | listener interno entre contenedores (`kafka-N:29092`)      |
| 8080          | Kafka UI, http://localhost:8080                            |

Comprobar la topologia por linea de comandos:

```bash
docker compose -f docker-compose.kafka.yml exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic innovatech.pedidos.eventos
```

Se espera `PartitionCount: 3`, `ReplicationFactor: 3` y, en cada particion, tres
brokers en `Replicas` y tres en `Isr`.

---

## 13. Pipeline de eventos del negocio

### 13.1 El recorrido

```
   POST /api/pedidos
          │
          ▼
  ┌─────────────────────┐
  │ ms-pedidos   :8082  │  guarda el pedido y publica el hecho
  │ PedidoService       │  por los DOS canales, en paralelo
  └───┬─────────────┬───┘
      │             │
      │ AMQP        │ Kafka (clave = pedidoId)
      ▼             ▼
 RabbitMQ      innovatech.pedidos.eventos
 (EP2, intacto)      │
                     ├──────────────────────────────┐
                     │ grupo-inventario             │ grupo-notificaciones
                     ▼                              ▼
          ┌─────────────────────┐        ┌─────────────────────┐
          │ ms-productos :8081  │        │ ms-notificaciones   │
          │ descuenta stock     │        │ avisa al cliente    │
          └──────────┬──────────┘        └─────────────────────┘
                     │ publica el movimiento
                     ▼
        innovatech.inventario.movimientos
                     │ grupo-alertas
                     ▼
          ┌─────────────────────┐
          │ ms-notificaciones   │  detecta stock bajo y registra la alerta
          └─────────────────────┘
```

`ms-productos` es a la vez **consumidor y productor**: lee de un topic, hace su
parte y escribe en otro. Es la forma habitual de encadenar etapas en una
arquitectura de streaming.

La ultima etapa es la demostracion concreta del valor del enfoque: nadie le pidio
a `ms-notificaciones` que revisara el stock. Reacciona solo, porque otro
microservicio publico un hecho y este estaba suscrito. Agregar manana un cuarto
interesado en los mismos movimientos **no obliga a tocar a `ms-productos`**:
basta que se suscriba con un grupo consumidor nuevo.

### 13.2 Los tres grupos consumidores

| Grupo                  | Topic                               | Microservicio       | Para que                        |
|------------------------|-------------------------------------|---------------------|---------------------------------|
| `grupo-inventario`     | `innovatech.pedidos.eventos`        | `ms-productos`      | descontar stock                 |
| `grupo-notificaciones` | `innovatech.pedidos.eventos`        | `ms-notificaciones` | avisar al cliente               |
| `grupo-alertas`        | `innovatech.inventario.movimientos` | `ms-notificaciones` | detectar stock bajo             |

Los dos primeros leen el **mismo** topic. Al tener grupos distintos, **ambos**
reciben todos los eventos, porque Kafka guarda un offset por grupo y cada grupo
recorre el topic completo por su cuenta.

Si compartieran grupo, Kafka repartiria las tres particiones entre ellos y cada
evento llegaria a uno solo de los dos. Eso es exactamente lo que se busca para
**balancear carga entre instancias del mismo microservicio**, y lo que hay que
evitar cuando son microservicios distintos con responsabilidades distintas.

Dentro de cada grupo, `setConcurrency(3)` levanta tres consumidores, uno por
particion: ahi si se reparte la carga.

### 13.3 Por que Kafka ademas de RabbitMQ

No es redundancia: cada tecnologia resuelve un problema distinto.

| | RabbitMQ (colas) | Kafka (log distribuido) |
|---|---|---|
| El mensaje tras ser leido | se retira de la cola | se conserva hasta que vence la retencion |
| Quien lleva la cuenta | el broker, por mensaje | el consumidor, con un offset por grupo |
| Reprocesar el historial | no se puede, ya no esta | se mueve el offset hacia atras |
| Agregar un interesado nuevo | hay que declarar cola y binding | se suscribe con un grupo nuevo |
| Mensaje defectuoso | se reencola o va a la **DLQ** | va al topic **`.DLT`** |
| Confirmacion | `basicAck` / `basicNack` por mensaje | el offset avanza si el listener no lanza |
| Fuerte en | trabajo pendiente, enrutamiento fino | historial de hechos, varios lectores, volumen |

En este proyecto el pedido necesita las dos cosas: un **trabajo** que alguien
debe ejecutar una vez (RabbitMQ) y un **hecho** que varios interesados quieren
conocer y poder releer (Kafka).

### 13.4 Estructura de los eventos

`PedidoEvento`, publicado por `ms-pedidos`:

| Campo        | Para que existe                                                  |
|--------------|------------------------------------------------------------------|
| `eventoId`   | identificador unico del evento; permite detectar duplicados      |
| `tipoEvento` | que ocurrio, por ejemplo `PEDIDO_CREADO`                         |
| `pedidoId`   | sirve tambien como **clave** del mensaje: fija la particion      |
| `cliente`    | lo usa `ms-notificaciones` para el aviso                         |
| `email`      | destino del aviso; su ausencia es un defecto permanente          |
| `items`      | producto y cantidad; lo usa `ms-productos` para descontar stock  |
| `simulacion` | fuerza un fallo a proposito, para probar el camino de error      |
| `fecha`      | cuando ocurrio el hecho                                          |

`MovimientoInventarioEvento`, publicado por `ms-productos`:

| Campo          | Para que existe                                                |
|----------------|----------------------------------------------------------------|
| `eventoId`     | identificador unico del movimiento                             |
| `tipoEvento`   | `STOCK_DESCONTADO`                                             |
| `pedidoId`     | enlaza con el evento que lo origino; vuelve a ser la clave     |
| `lineas`       | por producto: cantidad descontada, stock anterior y resultante |
| `hayStockBajo` | ya calculado por quien tiene el dato, para no repetir la regla |
| `fecha`        | cuando se aplico el movimiento                                 |

Cada evento lleva **lo que sus consumidores necesitan y nada mas**. El evento no
transporta el precio ni la direccion de despacho, porque ningun suscriptor los
usa: un evento que carga datos que nadie lee obliga a modificar al productor
cada vez que cambia un detalle ajeno.

El evento se publica con **`pedidoId` como clave**. Eso garantiza que todos los
eventos de un mismo pedido caigan en la misma particion y se procesen en orden.

### 13.5 Manejo de errores

| Pieza                             | Que hace                                                        |
|-----------------------------------|-----------------------------------------------------------------|
| `ErrorHandlingDeserializer`       | atrapa el JSON invalido antes de que llegue al listener         |
| `validar(...)` en los listeners   | rechaza el payload incompleto                                   |
| `FixedBackOff(2000, 2)`           | dos reintentos separados por dos segundos                       |
| `ErrorNoRecuperableException`     | marcada **no reintentable**: va directo al `.DLT`               |
| `DeadLetterPublishingRecoverer`   | publica en `<topic>.DLT` lo que fallo de forma definitiva        |
| `setRetryListeners(...)`          | deja en el log cada reintento con topic, particion y offset     |

Sin `ErrorHandlingDeserializer`, un mensaje con JSON invalido lanzaria la
excepcion **antes** de que el listener exista, y el contenedor quedaria
reintentando el mismo registro para siempre sin poder avanzar el offset.

`ErrorNoRecuperableException` se declara no reintentable porque un payload
invalido va a fallar igual las tres veces, y mientras se reintenta queda
**bloqueada la particion** para los mensajes que vienen detras.

Diferencia importante respecto de RabbitMQ: aqui el mensaje **no se devuelve a
la cola**. Kafka no borra ni reordena el registro; lo que avanza es el offset del
grupo. El reintento consiste en volver a entregar el mismo offset al listener
antes de avanzarlo.

---

## 14. `ms-kafka`: administracion y observabilidad

`ms-kafka` (puerto 8888) es la contraparte de `ms-admin-rabbit`: alla se
administran colas, aqui topics. Cumple tres papeles:

1. **Administrador**: declara la topologia al arrancar y expone crear, listar,
   describir, ampliar, configurar y eliminar topics.
2. **Observador**: publica por HTTP las cifras de particiones, offsets y retraso.
3. **Laboratorio**: mantiene un topic propio con productor y consumidor en el
   mismo proceso, para observar como la clave decide la particion.

No participa del pipeline de pedidos. Es una herramienta de operacion: si se
detiene, el negocio sigue funcionando.

### 14.1 Endpoints de administracion

| Metodo | Ruta                                               | Equivalente por CLI                       |
|--------|----------------------------------------------------|-------------------------------------------|
| GET    | `/api/kafka/admin/cluster`                         | `kafka-broker-api-versions`               |
| GET    | `/api/kafka/admin/topics`                          | `kafka-topics --list --describe`          |
| GET    | `/api/kafka/admin/topics/{nombre}`                 | `kafka-topics --describe --topic`         |
| POST   | `/api/kafka/admin/topics`                          | `kafka-topics --create`                   |
| PATCH  | `/api/kafka/admin/topics/{n}/particiones`          | `kafka-topics --alter --partitions`       |
| PATCH  | `/api/kafka/admin/topics/{n}/configuracion`        | `kafka-configs --alter`                   |
| DELETE | `/api/kafka/admin/topics/{nombre}`                 | `kafka-topics --delete`                   |
| GET    | `/api/kafka/admin/grupos`                          | `kafka-consumer-groups --describe`        |

### 14.2 Endpoints del laboratorio

| Metodo | Ruta                             | Proposito                                        |
|--------|----------------------------------|--------------------------------------------------|
| GET    | `/api/kafka/health`              | comprobacion de vida; no consulta al broker      |
| POST   | `/api/kafka/send`                | publica un registro (`message`, `key` opcional)  |
| GET    | `/api/kafka/recibidos`           | registros que leyo el consumidor                 |
| POST   | `/api/kafka/recibidos/limpiar`   | vacia la vista en memoria; no toca Kafka         |
| GET    | `/api/kafka/topico?topic=`       | `kafka-topics --describe`, de cualquier topic    |
| GET    | `/api/kafka/grupo?grupo=&topic=` | `kafka-consumer-groups --describe`, de cualquiera|

Se usa POST para publicar porque la operacion produce un efecto: escribe un
registro en el log. Un GET deberia poder repetirse sin consecuencias.

### 14.3 Restricciones deliberadas

No todo lo que el cliente de administracion permite tecnicamente conviene
permitirlo por HTTP:

- Los topics **internos** del cluster (los que empiezan con `__`, como
  `__consumer_offsets`) no se eliminan: borrarlos dejaria a todos los grupos
  consumidores sin su registro de avance.
- Los topics del **negocio** solo se eliminan con `confirmar=true`. Un clic
  accidental no debe poder borrar el historial de pedidos.
- Las particiones **solo se amplian**. Kafka no permite reducirlas, y el motivo
  es de fondo: el destino de una clave se calcula como
  `hash(clave) modulo particiones`, de modo que quitar particiones cambiaria el
  destino de las claves ya publicadas. Aumentarlas tampoco reubica lo ya
  escrito, asi que una misma clave puede quedar repartida en dos particiones:
  por eso en produccion el numero se dimensiona al crear el topic.

### 14.4 Pruebas

```bash
# Brokers vivos y coordinador. Debe informar 3 brokers.
curl.exe "http://localhost:8888/api/kafka/admin/cluster"

# Topics con particiones, replicas, Isr y configuracion.
curl.exe "http://localhost:8888/api/kafka/admin/topics"

# Crear un topic de prueba con su .DLT.
curl.exe -X POST "http://localhost:8888/api/kafka/admin/topics" -H "Content-Type: application/json" -d "{\"nombre\":\"innovatech.pruebas.admin\",\"particiones\":3,\"replicas\":3,\"crearDlt\":true}"

# Cambiar la retencion a una hora.
curl.exe -X PATCH "http://localhost:8888/api/kafka/admin/topics/innovatech.pruebas.admin/configuracion" -H "Content-Type: application/json" -d "{\"configuracion\":{\"retention.ms\":\"3600000\"}}"

# Ampliar a 6 particiones.
curl.exe -X PATCH "http://localhost:8888/api/kafka/admin/topics/innovatech.pruebas.admin/particiones" -H "Content-Type: application/json" -d "{\"particiones\":6}"

# Intentar reducir: debe responder 400 con la explicacion.
curl.exe -X PATCH "http://localhost:8888/api/kafka/admin/topics/innovatech.pruebas.admin/particiones" -H "Content-Type: application/json" -d "{\"particiones\":2}"

# Eliminar el topic de prueba.
curl.exe -X DELETE "http://localhost:8888/api/kafka/admin/topics/innovatech.pruebas.admin"

# Grupos consumidores con su retraso total.
curl.exe "http://localhost:8888/api/kafka/admin/grupos"
```

En Windows conviene usar `curl.exe` y no `curl`, porque en PowerShell `curl` es
un alias de `Invoke-WebRequest`, que interpreta los parametros de otra forma.

---

## 15. Orden de arranque y uso de memoria

Los tres archivos de Docker Compose **no se levantan todos a la vez**:
`docker-compose.yml` y `docker-compose.cluster.yml` usan los mismos puertos 5672
y 15672, por lo que son alternativas entre si.

| Archivo                     | Levanta                               | Memoria aprox. |
|-----------------------------|---------------------------------------|----------------|
| `docker-compose.yml`        | RabbitMQ de un nodo                   | ~0,4 GB        |
| `docker-compose.cluster.yml`| RabbitMQ de dos nodos en cluster      | ~0,8 GB        |
| `docker-compose.kafka.yml`  | 3 ZooKeeper + 3 brokers + Kafka UI    | ~2,6 GB        |

Secuencia recomendada en una maquina de 16 GB:

```bash
# Bloque 1: pruebas de RabbitMQ (EP2)
docker compose up -d
# ... pruebas ...
docker compose down

# Bloque 2: cluster de RabbitMQ
docker compose -f docker-compose.cluster.yml up -d
# ... pruebas ...
docker compose -f docker-compose.cluster.yml down

# Bloque 3: Kafka (EP3). Puede convivir con el bloque 1 si hace falta.
docker compose -f docker-compose.kafka.yml up -d
```

Los limites de memoria de cada contenedor estan fijados en los propios archivos
(`KAFKA_HEAP_OPTS`, `JAVA_OPTS`), de modo que el consumo no depende de lo que
decida la maquina virtual de Java por su cuenta.

Los microservicios de negocio **arrancan y funcionan aunque Kafka no este
disponible**: `PedidoService` publica por cada canal dentro de su propio bloque
`try/catch`, de modo que un fallo de Kafka queda en el log y no impide ni que el
pedido se guarde ni que el evento salga por RabbitMQ. Es la condicion que se
pedia: Kafka **complementa** la arquitectura existente sin afectar las
integraciones previas.

---

## 16. Despliegue en la nube

El sistema completo corre dentro de una instancia EC2 de 8 GB, con la base de
datos en Amazon RDS y Amazon API Gateway al frente como API Manager. Ninguna
pieza del codigo cambia entre el entorno local y el desplegado: lo unico que
cambia son las variables de entorno.

### 16.1 Variables de entorno

| Variable | Para que | Valor en local |
|---|---|---|
| `DB_URL` | cadena de conexion a MySQL | `jdbc:mysql://localhost:3306/innovatech` |
| `DB_USER` | usuario de la base de datos | `innovatech` |
| `DB_PASS` | clave de la base de datos | — |
| `CORS_ORIGINS` | origenes autorizados, separados por coma | `http://localhost:5173` |
| `RABBITMQ_HOST` | host del broker de colas | `localhost` |
| `KAFKA_BOOTSTRAP` | lista de contacto inicial de los brokers | `localhost:9092,localhost:9093,localhost:9094` |

### 16.2 Archivos del despliegue

| Archivo | Para que |
|---|---|
| `docker-compose.cluster.yml` | clúster de RabbitMQ de dos nodos |
| `docker-compose.kafka.yml` | tres ZooKeeper, tres brokers y la consola |
| `docker-compose.aws.yml` | superposicion con los limites de memoria de cada JVM |
| `arrancar.sh.ejemplo` | plantilla del script que levanta los cinco microservicios con el perfil `cloud` |

El archivo `arrancar.sh` real no esta versionado porque lleva la clave de la
base de datos. Se genera copiando la plantilla:

```bash
cp arrancar.sh.ejemplo arrancar.sh
nano arrancar.sh          # completar DB_URL, DB_PASS y CORS_ORIGINS
chmod +x arrancar.sh
./arrancar.sh
```

### 16.3 Orden de arranque en el servidor

```bash
docker compose -f docker-compose.cluster.yml up -d
docker compose -f docker-compose.kafka.yml -f docker-compose.aws.yml up -d

docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 \
  --create --topic innovatech.pedidos.eventos \
  --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2 --config retention.ms=604800000

docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 \
  --create --topic innovatech.inventario.movimientos \
  --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2 --config retention.ms=604800000

./arrancar.sh
```

Los microservicios usan el listener externo de los brokers (`localhost:909N`)
porque corren como procesos de la maquina y no dentro de la red de Docker. La
consola de administracion, que si es un contenedor, usa el listener interno.
