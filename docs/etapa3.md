# message-queue-ecommerce
## Etapa 3 — Configuração do RabbitMQ

**Trabalho Prático — Mensageria** · Sistemas Distribuídos · FURB
Prof. Gabriel Castellani · Entrega: 24/09/2026

---

## 1. Visão geral

| Item | Valor |
|---|---|
| Imagem | `rabbitmq:3.13-management` (`deploy/docker-compose.yml`) |
| Nós | 1 (sem cluster) |
| Virtual host | `/shop` |
| Exchanges | 4, todos `topic` e `durable` |
| Filas | 6, todas clássicas e `durable` |
| Políticas | 3 (`retry-dlx`, `retry-ttl`, `expiry-ttl`) |
| Listeners | **somente TLS**: `5671` (AMQP) e `15671` (Management HTTPS), publicados só em `127.0.0.1` |
| Usuários | `api`, `worker`, `monitor`, `admin` (sem `guest`) |

Toda a configuração do broker está em três arquivos de `deploy/`:

| Arquivo | Conteúdo | Versionado |
|---|---|---|
| `rabbitmq.conf` | Listeners TLS, certificados, `load_definitions`, intervalo de estatísticas | sim |
| `definitions.tmpl.json` | Usuários, vhost, permissões, **políticas, exchanges, filas e bindings** — com as senhas como marcadores (`__API_PASS__` …) | sim |
| `definitions.json` | O mesmo, com as senhas reais, gerado por `setup.sh` | **não** (`.gitignore`) |

A topologia é carregada **pelo broker** na inicialização (`load_definitions`),
não declarada pela aplicação. Consequências: (1) existe uma única fonte da
topologia — não há como dois componentes declararem a mesma fila com
argumentos diferentes e receberem `PRECONDITION_FAILED`; (2) um
`order.placed` publicado com o `worker` parado já encontra a fila `stock`; (3)
os usuários da aplicação não precisam de permissão de `configure` (§7).

![Arquitetura](diagramas/arquitetura.png)

<details>
<summary>Fonte Mermaid (<code>diagramas/arquitetura.mmd</code>)</summary>

```mermaid
flowchart TB
    CLI([Cliente]) -->|"POST /carts/:id/checkout"| API[api]
    API -->|publica order.placed| EX{{"exchange: orders — tipo topic"}}

    EX -->|"order.placed<br/>payment.declined<br/>reservation.expired"| QS[[fila: stock]]
    EX -->|reservation.created| QP[[fila: payment]]
    EX -->|"#  (todos os eventos)"| QN[[fila: notification]]

    subgraph W["processo worker — escalavel para N replicas"]
        direction LR
        HS[handler<br/>stock]
        HP[handler<br/>payment]
        HN[handler<br/>notification]
    end

    QS --> HS
    QP --> HP
    QN --> HN

    HS -.->|"publica reservation.created<br/>ou reservation.rejected"| EX
    HP -.->|"publica payment.approved<br/>ou payment.declined"| EX
    HS -.->|publica reservation.expired| XE{{exchange: expiry}}
    XE --> QX[["fila: expiry.q<br/>politica expiry-ttl: 120 s<br/>sem consumidor"]]
    QX -.->|"TTL expira, dead-letter<br/>routing key preservada"| EX

    QS & QP & QN -.->|"falha: Nack<br/>politica retry-dlx"| XR{{exchange: retry}}
    XR --> QR[["fila: retry.q<br/>politica retry-ttl: 10 s"]]
    QR -.->|"TTL expira, volta ao orders<br/>(todas as filas ligadas a chave)"| EX
    W -.->|"3a falha ou mensagem invalida<br/>publica com confirm"| XD{{exchange: dlx}}
    XD --> QD[["fila: dlq<br/>inspecao manual"]]

    API --> PG[(PostgreSQL)]
    HS --> PG
    HP --> PG

    classDef ex fill:#e8e8f5,stroke:#555
    classDef q fill:#f5f0e0,stroke:#555
    class EX,XE,XR,XD ex
    class QP,QS,QN,QX,QR,QD q
```

</details>

Valores que continuam no código (`internal/mq/mq.go`):

| Constante | Valor | Uso |
|---|---|---|
| `prefetch` | `10` | mensagens sem ack por consumidor |
| `maxAttempts` | `3` | entregas por fila antes da `dlq` |

## 2. Exchanges

| Exchange | Tipo | Durable | Papel |
|---|---|---|---|
| `orders` | `topic` | sim | Barramento principal: todo evento de negócio é publicado aqui |
| `retry` | `topic` | sim | Recebe as mensagens rejeitadas pelas filas de negócio (*dead-letter*) |
| `dlx` | `topic` | sim | Recebe as mensagens que esgotaram as tentativas ou são inválidas |
| `expiry` | `topic` | sim | Recebe o `reservation.expired` publicado junto com a reserva |

O tipo `topic` permite rotear por padrão de routing key. Isso é usado em
`notification` e nas filas técnicas, que recebem tudo com `#`.

## 3. Filas e políticas

As filas são declaradas sem argumentos; TTL e *dead-letter* vêm de
**políticas** (`policies` no `definitions.json`), aplicadas pelo broker por
padrão de nome:

| Política | Padrão | Definição | Filas afetadas |
|---|---|---|---|
| `retry-dlx` | `^(stock\|payment\|notification)$` | `dead-letter-exchange: retry` | `stock`, `payment`, `notification` |
| `retry-ttl` | `^retry\.q$` | `message-ttl: 10000`, `dead-letter-exchange: orders` | `retry.q` |
| `expiry-ttl` | `^expiry\.q$` | `message-ttl: 120000`, `dead-letter-exchange: orders` | `expiry.q` |

| Fila | Política | Consumidor | Por quê |
|---|---|---|---|
| `stock` | `retry-dlx` | `worker` (`stock.Handler`) | Falha vira retentativa via `retry` |
| `payment` | `retry-dlx` | `worker` (`payment.Handler`) | Idem |
| `notification` | `retry-dlx` | `worker` (`notification.Handler`) | Idem |
| `retry.q` | `retry-ttl` | nenhum | Segura a mensagem 10 s e a devolve ao `orders` |
| `expiry.q` | `expiry-ttl` | nenhum | Segura o `reservation.expired` por 120 s e o libera no `orders` |
| `dlq` | — | nenhum (inspeção manual) | Destino final de mensagens com falha |

`retry.q` e `expiry.q` **não têm consumidor de propósito**: elas funcionam como
temporizadores. Quando o TTL expira, o RabbitMQ faz *dead-letter* da mensagem
para o `orders` **preservando a routing key original** (nenhuma política
define `dead-letter-routing-key`), então ela chega de novo nas filas ligadas
àquela chave.

**Por que políticas e não argumentos de fila (`x-message-ttl`,
`x-dead-letter-exchange`).** Argumentos ficam gravados na fila: mudar o TTL
exige apagar e recriar a fila (perdendo as mensagens), e quem declara com um
valor diferente recebe `PRECONDITION_FAILED`. Políticas são aplicadas pelo
broker por cima da fila e podem ser alteradas a quente
(`rabbitmqctl set_policy`), sem redeclarar nada e sem mudar código. É também a
recomendação da documentação do RabbitMQ para TTL e DLX.

## 4. Bindings

| Origem | Binding key | Destino |
|---|---|---|
| `orders` | `order.placed` | `stock` |
| `orders` | `payment.declined` | `stock` |
| `orders` | `reservation.expired` | `stock` |
| `orders` | `reservation.created` | `payment` |
| `orders` | `#` | `notification` |
| `retry` | `#` | `retry.q` |
| `expiry` | `#` | `expiry.q` |
| `dlx` | `#` | `dlq` |

**Fan-out.** `notification` está ligada ao `orders` com `#`, então recebe uma
cópia de todo evento de negócio, além da fila que o processa. Um
`reservation.created`, por exemplo, chega em `payment` **e** em `notification`
com o mesmo `id`. Adicionar um novo interessado em eventos é só declarar uma
fila e um binding, sem mudar o produtor.

## 5. Propriedades das mensagens e publisher confirms

Toda publicação passa por `Publish` (`internal/mq/publish.go`):

| Propriedade | Valor | Por quê |
|---|---|---|
| routing key | `type` do envelope (ex.: `order.placed`) | O exchange `topic` roteia por ela |
| `delivery_mode` | `2` (`amqp.Persistent`) | A mensagem é gravada em disco e sobrevive ao reinício do broker (regra 3) |
| `content_type` | `application/json` | Corpo é o envelope JSON |
| `timestamp` | hora da publicação | Rastreio |
| `message_id` | `id` do envelope (UUID) | Mesma chave usada na idempotência (`processed_messages`) |
| `mandatory` | `false` | Toda routing key publicada tem fila ligada (garantido pelo `definitions.json`); uma mensagem sem rota seria descartada em silêncio (Etapa 5, Limitações) |

**Publisher confirms (regra 2).** Todo canal é aberto em modo confirm
(`OpenChannel` chama `Confirm(false)`). `Publish` só retorna sucesso depois do
`ack` do broker; `nack` ou 5 s sem resposta são tratados como falha. Mensagem
durável em fila durável (regra 3) só é garantia se o produtor souber que o
broker a recebeu — é isso que o confirm dá.

**Publicação dentro da transação.** Os três produtores (`api`, `stock`,
`payment`) publicam antes do `COMMIT` da transação que muda o pedido (Etapa 2,
§4).

**Checkout.** O `api` publica o `order.placed` **dentro** da transação que muda
o pedido de `CART` para `PLACED`. Se o confirm não chega (broker fora, `nack`
ou timeout), a transação é desfeita, o pedido continua `CART` e a resposta é
`503` — o cliente pode tentar de novo. O `201` só sai depois do confirm.

## 6. Consumo: prefetch, ack manual, retentativa e DLQ

| # | Regra | Valor | Onde | Por quê |
|---|---|---|---|---|
| 1 | Ack manual | `autoAck=false`; ack só depois do handler | `consume` | Um worker que cai no meio do processamento não perde a mensagem: ela volta para a fila |
| 5 | Prefetch | `Qos(10, 0, false)` | `consume` | Limita as mensagens sem ack por consumidor e distribui a carga entre workers |
| 8 | Um canal por consumidor | cada fila abre seu próprio canal; o handler publica nele | `consume` | Canais AMQP não são seguros para uso concorrente |
| 9 | Mensagem inválida → DLQ | JSON inválido ou `id`/`order_id` que não é UUID vai direto para `dlx` | `decode` | Retentar não conserta um corpo quebrado |
| 11 | Parada graciosa | `SIGTERM` para o consumo entre mensagens; o handler em andamento termina | `RunConsumers` | Um desligamento não interrompe uma transação no meio |
| 12 | Reconexão | espera de 1 s dobrando até 30 s, também entre tentativas que falham após conectar | `Connect`, `RunConsumers` | O broker pode reiniciar; o worker volta sozinho |
| 13 | Expiração da reserva | `reservation.expired` publicado no `expiry` dentro da transação da reserva | `stock` | O TTL de `expiry.q` vira o prazo da reserva; sem *confirm*, a reserva é desfeita |

![Retentativa e DLQ](diagramas/retentativa.png)

<details>
<summary>Fonte Mermaid (<code>diagramas/retentativa.mmd</code>)</summary>

```mermaid
flowchart LR
    Q[["fila de negocio<br/>(stock / payment / notification)"]]
    Q -->|handler processa| OK([sucesso: ack])
    Q -.->|"falha: Nack(requeue=false)<br/>politica retry-dlx"| RX{{exchange: retry}}
    RX --> RQ[["fila: retry.q<br/>politica retry-ttl: 10 s"]]
    RQ -.->|"TTL expira, dead-letter<br/>routing key preservada"| EX{{exchange: orders}}
    EX -.->|"volta a TODAS as filas ligadas a chave<br/>(duplicatas absorvidas por processed_messages)"| Q
    Q -.->|"3a falha (header x-death)<br/>ou JSON/UUID invalido"| DX{{exchange: dlx}}
    DX --> DLQ[["fila: dlq<br/>inspecao manual"]]

    classDef ex fill:#e8e8f5,stroke:#555
    classDef q fill:#f5f0e0,stroke:#555
    class RX,EX,DX ex
    class Q,RQ,DLQ q
```

</details>

**Ciclo de retentativa.** Quando o handler falha:

1. O `worker` rejeita a mensagem sem requeue (`Nack(false, false)`).
2. A fila de negócio (`stock`, `payment` ou `notification`) faz *dead-letter*
   para o exchange `retry`, que entrega em `retry.q`.
3. Após 10 s (política `retry-ttl`), `retry.q` faz *dead-letter* para o
   `orders` com a routing key original.
4. A mensagem volta para as filas ligadas àquela routing key.

**Contagem de tentativas.** São **3 tentativas no total** (não 3
retentativas). A cada rejeição o RabbitMQ atualiza o header `x-death` da
mensagem. `deathCount` conta apenas a entrada com `queue` igual à própria fila
e `reason` igual a `rejected` — cada ciclo também adiciona uma entrada
`retry.q`/`expired`, que é ignorada. Na 3ª falha a contagem já é 2, então a
mensagem é republicada no `dlx` (com confirm) e só então recebe ack. Se essa
publicação falhar, a mensagem volta para a retentativa em vez de se perder.

**Cópias na retentativa.** Como a retentativa devolve a mensagem ao `orders`,
ela chega **de novo em todas as filas ligadas** àquela routing key: uma falha
em `payment` faz o `notification` registrar o mesmo evento mais uma vez. Isso
é correto porque os handlers com efeito no banco são idempotentes (tabela
`processed_messages`) e toda mudança de status é um compare-and-set. A
contagem por fila mantém cada cópia independente.

**`FAIL_RATE`.** Variável de ambiente do `worker` (padrão `0`) que simula erro
do gateway de pagamento. Afeta **somente** o `payment`, antes de qualquer
escrita no banco, e serve para demonstrar a retentativa e a DLQ
(`FAIL_RATE=1 docker compose up -d worker`).

## 7. Autenticação e autorização

### 7.1 Usuários

Usuários, vhost e permissões vêm do `definitions.json`, carregado na
inicialização (`load_definitions` no `rabbitmq.conf`). Com definições
importadas no boot, o RabbitMQ **não cria** o usuário padrão `guest`.

| Usuário | Tag | Quem usa | Senha |
|---|---|---|---|
| `api` | nenhuma | processo `api` (`AMQP_URL`) | `API_PASS` em `deploy/.env` |
| `worker` | nenhuma | processo `worker` (`AMQP_URL`) | `WORKER_PASS` |
| `monitor` | `monitoring` | Mapa de Mensagens (`web/`, `MQ_USER`/`MQ_PASS`) | `MONITOR_PASS` |
| `admin` | `administrator` | operador humano: Management UI, inspeção e republicação da DLQ | `ADMIN_PASS` |

**Senhas.** `deploy/setup.sh` gera uma senha aleatória de 128 bits por usuário
(`openssl rand -hex 16`) em `deploy/.env`, e renderiza `definitions.json` e
`web/.env.local` a partir dela. O Compose lê o `.env` e monta as URLs
(`amqps://api:${API_PASS}@rabbitmq:5671/%2Fshop`). Nenhuma senha fica no
repositório: `.env`, `definitions.json`, `certs/` e `web/.env.local` estão no
`.gitignore`. Cada componente tem credencial própria, então uma credencial
vazada não dá acesso às funções dos outros.

### 7.2 Permissões (menor privilégio)

| Usuário | `configure` | `write` | `read` |
|---|---|---|---|
| `api` | `^$` | `^orders$` | `^$` |
| `worker` | `^$` | `^(orders\|expiry\|dlx)$` | `^(stock\|payment\|notification)$` |
| `monitor` | `^$` | `^$` | `^dlq$` |
| `admin` | `.*` | `.*` | `.*` |

Por que cada regex é a mínima:

- **`configure ^$` para todos os componentes.** Ninguém declara nada: a
  topologia vem do `definitions.json` (§1).
- **`api`** só publica `order.placed` no `orders`. Não consome nada.
- **`worker`** consome das três filas de negócio (`read`) e publica no `orders`
  (eventos), no `expiry` (prazo da reserva) e no `dlx` (mensagens que esgotaram
  as tentativas). Não precisa de `write` no `retry`: o *dead-lettering* de uma
  fila para o `retry` é feito pelo próprio broker, sem checagem de permissão do
  usuário.
- **`monitor`** lê estatísticas pela Management API (tag `monitoring`, sem
  permissão de vhost) e só faz a "espiada" da `dlq` (um `get` com requeue), que
  exige `read` nela. Não publica nem apaga nada.
- **`admin`** é o único com poderes amplos, e só é usado por pessoas.

Verificado na Etapa 4 (caso 17): `api` publicando no `retry`, `dlx` ou `expiry`
recebe `ACCESS_REFUSED`, assim como `worker` no `retry`; `monitor` não consegue
publicar nem esvaziar fila; `guest` e `api` recebem `401` na Management API.

![Usuários, permissões e TLS](diagramas/seguranca.png)

<details>
<summary>Fonte Mermaid (<code>diagramas/seguranca.mmd</code>)</summary>

```mermaid
flowchart LR
    subgraph C["clientes"]
        A[api]
        W[worker]
        M[web: Mapa de Mensagens]
        O([operador])
    end
    subgraph B["RabbitMQ — vhost /shop"]
        L1["listener AMQP TLS :5671<br/>(5672 desligada)"]
        L2["Management HTTPS :15671<br/>(15672 desligada)"]
        XO{{orders}}
        XE{{expiry}}
        XD{{dlx}}
        QB[[stock · payment · notification]]
        QD[[dlq]]
    end
    A -->|"amqps, usuario api"| L1
    W -->|"amqps, usuario worker"| L1
    M -->|"https, usuario monitor"| L2
    O -->|"https, usuario admin"| L2
    L1 -.->|"api: write ^orders$"| XO
    L1 -.->|"worker: write ^(orders|expiry|dlx)$"| XE
    L1 -.->|"worker: write"| XD
    L1 -.->|"worker: read ^(stock|payment|notification)$"| QB
    L2 -.->|"monitor: read ^dlq$ (tag monitoring)"| QD
```

</details>

## 8. Criptografia (TLS)

TLS está **habilitado e é o único caminho**: o listener AMQP sem criptografia
está desligado (`listeners.tcp = none`) e a Management API só existe em HTTPS.

### 8.1 Certificados

`deploy/setup.sh` cria uma CA própria e um certificado do servidor assinado por
ela, com *Subject Alternative Name* para os dois nomes pelos quais o broker é
acessado (`rabbitmq` dentro do Compose e `localhost` no host):

```sh
openssl req -x509 -newkey rsa:2048 -nodes -days 365 -subj "/CN=shop-ca" \
  -keyout certs/ca.key -out certs/ca.pem
openssl req -newkey rsa:2048 -nodes -subj "/CN=rabbitmq" \
  -keyout certs/server.key -out certs/server.csr
printf 'subjectAltName=DNS:rabbitmq,DNS:localhost\n' > certs/san.ext
openssl x509 -req -in certs/server.csr -CA certs/ca.pem -CAkey certs/ca.key -CAcreateserial \
  -days 365 -extfile certs/san.ext -out certs/server.pem
```

O SAN é obrigatório: o cliente Go rejeita certificados que só têm o nome no
`CN`.

### 8.2 Servidor (`deploy/rabbitmq.conf`)

```ini
listeners.tcp = none
listeners.ssl.default = 5671
ssl_options.cacertfile = /etc/rabbitmq/certs/ca.pem
ssl_options.certfile = /etc/rabbitmq/certs/server.pem
ssl_options.keyfile = /etc/rabbitmq/certs/server.key
ssl_options.verify = verify_peer
ssl_options.fail_if_no_peer_cert = false
ssl_options.versions.1 = tlsv1.3
ssl_options.versions.2 = tlsv1.2

management.ssl.port = 15671
management.ssl.cacertfile = /etc/rabbitmq/certs/ca.pem
management.ssl.certfile = /etc/rabbitmq/certs/server.pem
management.ssl.keyfile = /etc/rabbitmq/certs/server.key

load_definitions = /etc/rabbitmq/definitions.json
collect_statistics_interval = 1000
```

- Só TLS 1.3 e 1.2; versões antigas são recusadas.
- `fail_if_no_peer_cert = false`: o cliente não precisa de certificado próprio
  (a autenticação do cliente é por usuário e senha, já cifrados pelo TLS).
- O Compose publica as portas apenas em `127.0.0.1` (`127.0.0.1:5671:5671`,
  `127.0.0.1:15671:15671`): o broker não é alcançável por outras máquinas da
  rede. As portas `5672` e `15672` não existem.
- `collect_statistics_interval = 1000` (e `sample_retention_policies`, em
  `RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS`) só afetam a Management API: amostras a
  cada 1 s em vez de 5 s, para o Mapa de Mensagens.

### 8.3 Clientes

| Cliente | Como confia na CA | URL |
|---|---|---|
| `api` e `worker` (Go) | `SSL_CERT_FILE=/certs/ca.pem` (o `ca.pem` é montado no contêiner); a biblioteca padrão do Go lê essa variável, sem código de TLS na aplicação | `amqps://…@rabbitmq:5671/%2Fshop` |
| Mapa de Mensagens (Node) | `NODE_EXTRA_CA_CERTS=../deploy/certs/ca.pem` nos scripts `dev`/`start` | `https://localhost:15671` |
| Operador (curl / navegador) | `curl --cacert deploy/certs/ca.pem`; o navegador alerta sobre a CA própria | `https://localhost:15671` |

O cliente verifica o certificado e o nome do servidor (`rabbitmq` ou
`localhost`, pelo SAN). Evidência na Etapa 4 (caso 17): `openssl s_client`
negocia `TLSv1.3` com `TLS_AES_256_GCM_SHA384` e `Verify return code: 0 (ok)`
nas duas portas; `5672` e `15672` recusam conexão.

## 9. Limitações

| Limitação | Impacto | Quando resolver |
|---|---|---|
| CA própria, sem mTLS | Clientes se autenticam por senha (dentro do TLS), não por certificado | Ambiente com requisito de identidade forte: `fail_if_no_peer_cert = true` e certificado por cliente |
| PostgreSQL sem TLS | O tráfego `api`/`worker` ↔ banco fica em texto claro, mas só na rede interna do Compose (a porta `5432` não é publicada) | Banco fora da rede do Compose: `sslmode=verify-full` |
| Um único nó, filas clássicas | O broker é ponto único de falha; sem réplica das filas | Com requisito de alta disponibilidade: cluster de 3 nós e *quorum queues* |
| `dlq` sem limite | Mensagens com falha se acumulam até alguém inspecionar | Política com `max-length` na `dlq` e alerta de monitoramento |
| Um nível de retentativa | Espera fixa de 10 s em todas as tentativas | Se falhas longas forem comuns: filas com TTL crescente (backoff exponencial) |
