# message-queue-ecommerce
## Etapa 3 — Configuração do RabbitMQ

**Trabalho Prático — Mensageria** · Sistemas Distribuídos · FURB
Prof. Gabriel Castellani · Entrega: a definir

---

## 1. Visão geral

| Item | Valor |
|---|---|
| Imagem | `rabbitmq:3.13-management` (`deploy/docker-compose.yml`) |
| Nós | 1 (sem cluster) |
| Virtual host | `/shop` |
| Exchanges | 4, todos `topic` e `durable` |
| Filas | 6, todas clássicas e `durable` |
| Portas no host | `5672` (AMQP) e `15672` (Management UI) |
| Usuários | `api` e `worker` (sem `guest`) |

A topologia (exchanges, filas, argumentos e bindings) é declarada **pela
aplicação**, não pelo `definitions.json`: tanto o `api` quanto o `worker` chamam
`DeclareTopology` ao iniciar (`internal/mq/publish.go`). A declaração é
idempotente — os argumentos são sempre os mesmos, então redeclarar não gera
`PRECONDITION_FAILED`. Como o `api` também declara, um `order.placed` publicado
com o `worker` parado já encontra a fila `stock` e não é descartado como
mensagem sem rota.

Todos os valores deste documento vêm das constantes em `internal/mq/mq.go`:

| Constante | Valor | Uso |
|---|---|---|
| `prefetch` | `10` | mensagens sem ack por consumidor |
| `maxAttempts` | `3` | entregas por fila antes da `dlq` |
| `retryTTL` | `10000` ms | espera em `retry.q` antes de voltar |
| `expiryTTL` | `120000` ms | validade da reserva em `expiry.q` |

## 2. Exchanges

| Exchange | Tipo | Durable | Papel |
|---|---|---|---|
| `orders` | `topic` | sim | Barramento principal: todo evento de negócio é publicado aqui |
| `retry` | `topic` | sim | Recebe as mensagens rejeitadas pelas filas de negócio (*dead-letter*) |
| `dlx` | `topic` | sim | Recebe as mensagens que esgotaram as tentativas ou são inválidas |
| `expiry` | `topic` | sim | Recebe o `reservation.expired` publicado junto com a reserva |

O tipo `topic` permite rotear por padrão de routing key. Isso é usado em
`notification` e nas filas técnicas, que recebem tudo com `#`.

## 3. Filas e argumentos

| Fila | `x-message-ttl` | `x-dead-letter-exchange` | Consumidor | Por quê |
|---|---|---|---|---|
| `stock` | — | `retry` | `worker` (`stock.Handler`) | Falha vira retentativa via `retry` |
| `payment` | — | `retry` | `worker` (`payment.Handler`) | Idem |
| `notification` | — | `retry` | `worker` (`notification.Handler`) | Idem |
| `retry.q` | `10000` | `orders` | nenhum | Segura a mensagem 10 s e a devolve ao `orders` |
| `expiry.q` | `120000` | `orders` | nenhum | Segura o `reservation.expired` por 120 s e o libera no `orders` |
| `dlq` | — | — | nenhum (inspeção manual) | Destino final de mensagens com falha |

`retry.q` e `expiry.q` **não têm consumidor de propósito**: elas funcionam como
temporizadores. Quando o TTL expira, o RabbitMQ faz *dead-letter* da mensagem
para o `orders` **preservando a routing key original** (não há
`x-dead-letter-routing-key` em nenhuma fila), então ela chega de novo nas filas
certas.

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

**Publisher confirms (regra 2).** Todo canal é aberto em modo confirm
(`OpenChannel` chama `Confirm(false)`). `Publish` só retorna sucesso depois do
`ack` do broker; `nack` ou 5 s sem resposta são tratados como falha. Mensagem
durável em fila durável (regra 3) só é garantia se o produtor souber que o
broker a recebeu — é isso que o confirm dá.

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
| 13 | Expiração da reserva | `reservation.expired` publicado no `expiry` junto com a reserva | `stock` | O TTL de `expiry.q` vira o prazo da reserva |

**Ciclo de retentativa.** Quando o handler falha:

1. O `worker` rejeita a mensagem sem requeue (`Nack(false, false)`).
2. A fila de negócio (`stock`, `payment` ou `notification`) faz *dead-letter*
   para o exchange `retry`, que entrega em `retry.q`.
3. Após 10 s (`x-message-ttl: 10000`), `retry.q` faz *dead-letter* para o
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

## 7. Usuários, permissões e vhost

Usuários, vhost e permissões vêm de `deploy/definitions.json`, carregado na
inicialização do broker via `load_definitions`:

```yaml
# deploy/docker-compose.yml (serviço rabbitmq)
volumes: ["./definitions.json:/etc/rabbitmq/definitions.json:ro"]
environment:
  RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS: '-rabbit load_definitions "/etc/rabbitmq/definitions.json"'
```

```json
"users": [
  {"name": "api", "password": "api", "tags": ""},
  {"name": "worker", "password": "worker", "tags": "management"}
],
"vhosts": [{"name": "/shop"}],
"permissions": [
  {"user": "api", "vhost": "/shop",
   "configure": "^(orders|retry|dlx|expiry|stock|payment|notification|retry\\.q|expiry\\.q|dlq)$",
   "write": "<mesma regex>", "read": "<mesma regex>"}
]
```

| Usuário | Tags | Permissões em `/shop` | Uso |
|---|---|---|---|
| `api` | nenhuma | configure/write/read na regex acima | Publica `order.placed` |
| `worker` | `management` | configure/write/read na regex acima | Consome e publica; login na Management UI |

- **Por que as mesmas permissões para os dois.** Os dois declaram a topologia
  no boot. Declarar uma fila com `x-dead-letter-exchange` e criar bindings
  exige, além de `configure`, permissão de `read` e `write` nos exchanges e
  filas envolvidos; só `configure` falha com `ACCESS_REFUSED`. A regex fica
  restrita aos 10 nomes da topologia. **Trade-off:** a regra "o `api` só
  escreve no `orders`" é garantida pelo código, não pelo broker.
- **Management UI.** Só o `worker` tem a tag `management`: o login em
  `http://localhost:15672` é `worker`/`worker`. O `api` recebe `401`.
- **Sem `guest`.** Com as definições importadas no boot, o RabbitMQ não cria o
  usuário padrão `guest` (login retorna `401`).
- **URL de conexão.** O nome do vhost `/shop` é codificado na URL como
  `%2Fshop`: `AMQP_URL=amqp://worker:worker@rabbitmq:5672/%2Fshop` (e
  `amqp://api:api@…` no `api`).

## 8. TLS (porta 5671)

TLS está **documentado, mas não habilitado**. A configuração abaixo mostra como
seria ativado.

1. Gerar uma CA própria e um certificado do servidor assinado por ela:

   ```sh
   openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
     -keyout ca_key.pem -out ca_cert.pem -subj "/CN=shop-ca"
   openssl req -newkey rsa:2048 -nodes \
     -keyout server_key.pem -out server.csr -subj "/CN=rabbitmq"
   openssl x509 -req -in server.csr -CA ca_cert.pem -CAkey ca_key.pem \
     -CAcreateserial -out server_cert.pem -days 365
   ```

2. Habilitar o listener TLS no `rabbitmq.conf`:

   ```ini
   listeners.ssl.default = 5671
   ssl_options.cacertfile = /etc/rabbitmq/certs/ca_cert.pem
   ssl_options.certfile   = /etc/rabbitmq/certs/server_cert.pem
   ssl_options.keyfile    = /etc/rabbitmq/certs/server_key.pem
   ssl_options.verify     = verify_peer
   ssl_options.fail_if_no_peer_cert = false
   ```

3. Clientes passam a usar `amqps` na porta 5671:
   `amqps://worker:worker@rabbitmq:5671/%2Fshop`.

**Por que desligado.** Aqui tudo roda em `localhost`, dentro da rede do
Compose. Um certificado autoassinado nesse cenário demonstra a geração, não
segurança real, e adicionaria arquivos e passos ao ambiente de avaliação. Em
produção o TLS seria obrigatório.

## 9. Limitações

| Limitação | Impacto | Quando resolver |
|---|---|---|
| TLS desligado | Credenciais e mensagens trafegam em texto claro | Qualquer ambiente fora de `localhost` |
| Um único nó, filas clássicas | O broker é ponto único de falha; sem réplica das filas | Com requisito de alta disponibilidade: cluster de 3 nós e *quorum queues* |
| Permissões amplas | `api` pode, pelo broker, ler e escrever em toda a topologia (seção 7) | Declarar a topologia só no `worker` ou no `definitions.json` e restringir o `api` a `write` no `orders` |
| Um nível de retentativa | Espera fixa de 10 s em todas as tentativas | Se falhas longas forem comuns: filas com TTL crescente (backoff exponencial) |
