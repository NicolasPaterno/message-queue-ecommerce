# message-queue-ecommerce
## Etapa 4: Exemplos de Uso

**Trabalho Prático de Mensageria** · Sistemas Distribuídos · FURB
Prof. Gabriel Castellani · Entrega: 24/09/2026

---

Executamos 17 casos de uso contra o ambiente real e copiamos aqui as saídas obtidas,
cortadas nas linhas que importam. UUIDs repetidos foram abreviados com `…`.

| Item | Valor |
|---|---|
| Máquina | macOS 26.6.2 (`arm64`), Docker 29.4.2, Compose v5.1.3 |
| Execução | 01/10/2026, das 18:15 às 18:23 UTC, em sequência, a partir de um ambiente zerado |
| Commit | `a495477`; o caso 15 rodou no `e4e1e5a`, que limita o pool de conexões |

Os comandos rodam em `deploy/` com estas funções auxiliares:

```sh
. ./.env
sql()   { docker compose exec -T postgres psql -U shop -d shop -tAc "$1"; }
mq()    { curl -s --cacert certs/ca.pem -u admin:$ADMIN_PASS "https://localhost:15671/api/$1" "${@:2}"; }
P=$(sql "SELECT id FROM products WHERE name='Keyboard'")
order() { C=$(curl -s -X POST localhost:8080/carts | jq -r .id)
  curl -s -X POST localhost:8080/carts/$C/items -d "{\"product_id\":\"$P\",\"quantity\":${1:-1}}" >/dev/null
  curl -s -X POST localhost:8080/carts/$C/checkout; echo; }
st()    { curl -s localhost:8080/orders/$1 | jq -r .status; }
stock() { sql "UPDATE products SET available=$1 WHERE id='$P'"; }
avail() { sql "SELECT available FROM products WHERE id='$P'"; }
pub()   { mq exchanges/%2Fshop/orders/publish -X POST -H 'content-type: application/json' \
  -d "$(jq -nc --arg k "$1" --arg p "$2" '{properties:{delivery_mode:2,content_type:"application/json"},routing_key:$k,payload:$p,payload_encoding:"string"}')"; echo; }
```

`pub` publica no `orders` pela Management UI com o usuário `admin`. Usamos para
simular uma mensagem repetida e uma mensagem inválida.

| Caso | O que mostra |
|---|---|
| 1 | Pedido pago; o checkout não espera o processamento |
| 2 | Pagamento recusado devolve o estoque |
| 3 | Sem estoque, ninguém é cobrado |
| 4 | Dois clientes disputam a última unidade |
| 5 | A reserva expira em 120 s |
| 6 | Fan-out: um evento, duas filas |
| 7 | Retentativa a cada 10 s |
| 8 | DLQ depois de 3 tentativas |
| 9 | `worker` parado e broker reiniciado sem perder mensagens |
| 10 | Três réplicas dividindo o trabalho |
| 11 | Mensagem repetida não tem efeito duplicado |
| 12 | Broker fora no checkout |
| 13 | Erros de entrada na API |
| 14 | Mensagem inválida vai direto para a DLQ |
| 15 | `worker` morto com `kill -9` no meio de 200 pedidos |
| 16 | Pagamento que chega depois da expiração |
| 17 | TLS, usuários e permissões |

**Mensagens do caso 1.** Todas têm `delivery_mode: 2`, `content_type: application/json`
e `message_id` igual ao `id`.

| Exchange e *routing key* | Produtor | Filas | Corpo |
|---|---|---|---|
| `orders`, `order.placed` | `api` | `stock`, `notification` | `{"id":"5ee6dcd2-…","type":"order.placed","order_id":"d93c8d8e-…","data":{}}` |
| `orders`, `reservation.created` | `stock` | `payment`, `notification` | `{"id":"9750cde6-…","type":"reservation.created","order_id":"d93c8d8e-…","data":{}}` |
| `expiry`, `reservation.expired` | `stock` | `expiry.q`, e 120 s depois `stock` e `notification` | `{"id":"532d24bb-…","type":"reservation.expired","order_id":"d93c8d8e-…","data":{}}` |
| `orders`, `payment.approved` | `payment` | `notification` | `{"id":"85108112-…","type":"payment.approved","order_id":"d93c8d8e-…","data":{"transaction_id":"TX-449a84f9","amount_cents":24990}}` |

---

### Caso 1: Pedido pago

![Fluxo de um pedido pago](diagramas/sequencia-feliz.png)

**Entrada**
```sh
stock 10
C=$(curl -s -X POST localhost:8080/carts | jq -r .id)
curl -s -X POST localhost:8080/carts/$C/items -d "{\"product_id\":\"$P\",\"quantity\":1}"
curl -s -w ' HTTP %{http_code} %{time_total}s\n' -X POST localhost:8080/carts/$C/checkout
sleep 3; curl -s localhost:8080/orders/$C; avail; C1=$C
docker compose logs --no-log-prefix worker | grep "order_id=$C1" | grep -E "stock:|payment:|notification:"

# o mesmo checkout com o worker parado
docker compose stop worker
C=$(curl -s -X POST localhost:8080/carts | jq -r .id)
curl -s -X POST localhost:8080/carts/$C/items -d "{\"product_id\":\"$P\",\"quantity\":1}" >/dev/null
curl -s -o /dev/null -w '%{http_code} %{time_total}\n' -X POST localhost:8080/carts/$C/checkout
st $C; docker compose start worker; sleep 6; st $C
```

**Processamento.** A `api` muda o pedido para `PLACED`, publica `order.placed`, espera
o *confirm* e responde. O `stock` reserva e publica `reservation.created` e
`reservation.expired`. O `payment` aprova e publica `payment.approved`. O
`notification` recebe os três eventos.

**Saída**
```
{"id":"d93c8d8e-e941-4958-bd36-364fa06fbebc","status":"PLACED"} HTTP 201 0.005342s
{"id":"d93c8d8e-…","items":[{"product_id":"5b8909f6-…","quantity":1,"price_cents":24990}],"status":"PAID","total_cents":24990}
available=9
18:19:43 stock: reserved order_id=d93c8d8e-…
18:19:43 payment: approved order_id=d93c8d8e-… transaction_id=TX-449a84f9 amount_cents=24990
18:19:43 notification: type=payment.approved id=85108112-… order_id=d93c8d8e-…

com o worker parado: 201 em 0,006 s, pedido PLACED; ao religar o worker, PAID
```
O checkout respondeu em 5 ms, e com o `worker` parado o pedido ficou esperando na fila
até ele voltar.

### Caso 2: Pagamento recusado

**Entrada**
```sh
stock 100; for i in $(seq 20); do order 1 >/dev/null; done; sleep 8
sql "SELECT status, count(*) FROM orders WHERE status IN ('PAID','DECLINED') GROUP BY 1"
D=$(sql "SELECT id FROM orders WHERE status='DECLINED' ORDER BY updated_at DESC LIMIT 1")
docker compose logs --no-log-prefix worker | grep -E "declined order_id=$D|released order_id=$D"; avail
```

**Processamento.** O `payment` recusa ~15 % ao acaso e publica `payment.declined`. O
`stock` muda o pedido de `RESERVED` para `DECLINED` e devolve as unidades.

**Saída**
```
DECLINED|3
PAID|18
18:15:23 payment: declined order_id=20bc6c73-…
18:15:23 stock: released order_id=20bc6c73-… status=DECLINED
available=83
```
Dos 20 pedidos, 17 foram pagos e 3 recusados (o 18º pago é do caso 1).
`100 − 17 = 83`: as unidades recusadas voltaram.

### Caso 3: Sem estoque

![Sem estoque e última unidade](diagramas/sequencia-estoque.png)

**Entrada**
```sh
stock 10; order 11; sleep 3; st $C
docker compose logs worker | grep "order_id=$C" | grep -c queue=payment
docker compose logs --no-log-prefix worker | grep "order_id=$C" | grep -E "stock:|notification:"; avail
```

**Processamento.** O `UPDATE ... AND available >= 11` não muda nenhuma linha, e a
transação é desfeita. Numa nova transação o pedido vira `OUT_OF_STOCK` e o `stock`
publica `reservation.rejected`, que só o `notification` recebe.

**Saída**
```
{"id":"bbeebb25-…","status":"PLACED"}
status=OUT_OF_STOCK
linhas do payment para esse pedido: 0
18:15:32 stock: out_of_stock order_id=bbeebb25-…
18:15:32 notification: type=reservation.rejected id=6c9e7590-… order_id=bbeebb25-…
available=10
```

### Caso 4: Última unidade

**Entrada**
```sh
stock 1
A=$(curl -s -X POST localhost:8080/carts | jq -r .id); B=$(curl -s -X POST localhost:8080/carts | jq -r .id)
for c in $A $B; do curl -s -X POST localhost:8080/carts/$c/items -d "{\"product_id\":\"$P\",\"quantity\":1}" >/dev/null; done
curl -s -X POST localhost:8080/carts/$A/checkout & curl -s -X POST localhost:8080/carts/$B/checkout & wait; sleep 4
echo "C1=$(st $A) C2=$(st $B) available=$(avail)"
docker compose logs --no-log-prefix worker | grep -E "stock: .*order_id=($A|$B)"
```

**Processamento.** Os dois `order.placed` chegam ao `stock`. O PostgreSQL executa os
dois `UPDATE` na mesma linha um depois do outro: o primeiro muda 1 linha, o segundo
encontra `available = 0` e não muda nada.

**Saída**
```
C1=OUT_OF_STOCK C2=PAID available=0
18:15:35 stock: reserved order_id=49a1de5e-…
18:15:35 stock: out_of_stock order_id=b9f49e4b-…
```

### Caso 5: Reserva expirada

**Entrada**
```sh
FAIL_RATE=1 docker compose up -d worker; sleep 4
stock 10; order 1; C5=$C; sleep 2; st $C5; avail
sleep 125; st $C5; avail
docker compose logs --no-log-prefix worker | grep "order_id=$C5" | grep -E "stock: (reserved|released)"
```

**Processamento.** Com `FAIL_RATE=1` o pagamento nunca conclui (casos 7 e 8). O
`reservation.expired` espera 120 s na `expiry.q` e volta ao `stock`, que encontra o
pedido ainda `RESERVED`, muda para `EXPIRED` e devolve a unidade.

**Saída**
```
status=RESERVED available=9   (27 s após o checkout)
status=EXPIRED  available=10  (122 s após o checkout)
18:17:02 stock: reserved order_id=921994ea-…
18:19:02 stock: released order_id=921994ea-… status=EXPIRED
```
Reserva às 18:17:02 e liberação às 18:19:02, exatamente 120 s depois.

### Caso 6: Fan-out

**Entrada**
```sh
docker compose logs --no-log-prefix worker | grep "type=reservation.created" | grep "order_id=$C1" | grep "queue="
```

**Processamento.** O `reservation.created` é publicado uma vez. O binding
`reservation.created` entrega ao `payment`, e o binding `#` entrega ao `notification`.

**Saída**
```
18:19:43 queue=notification type=reservation.created id=9750cde6-209d-4a5d-b1cb-df423468ff21 attempt=1
18:19:43 queue=payment      type=reservation.created id=9750cde6-209d-4a5d-b1cb-df423468ff21 attempt=1
```
A mesma mensagem, com o mesmo `id`, nas duas filas.

### Caso 7: Retentativa

![Retentativa e DLQ](diagramas/sequencia-falha.png)

**Entrada** (pedido do caso 5, ainda com `FAIL_RATE=1`, uns 25 s depois do checkout)
```sh
docker compose logs --no-log-prefix worker | grep "order_id=$C5" | grep "queue=payment type="
docker compose logs --no-log-prefix worker | grep "queue=payment" | grep -E "failed, retry|-> dlq"
```

**Processamento.** O `payment` falha e rejeita a mensagem. A política manda a mensagem
para o `retry`, ela espera 10 s na `retry.q` e volta ao `orders`. A cada volta o
`notification` também recebe a mensagem de novo.

**Saída**
```
18:17:02 queue=payment type=reservation.created id=23a55896-… attempt=1
18:17:02 queue=payment id=23a55896-… attempt=1 failed, retry: simulated gateway error
18:17:12 queue=payment type=reservation.created id=23a55896-… attempt=2
18:17:12 queue=payment id=23a55896-… attempt=2 failed, retry: simulated gateway error
18:17:22 queue=payment type=reservation.created id=23a55896-… attempt=3
18:17:22 queue=payment id=23a55896-… -> dlq: simulated gateway error
```
As tentativas ficaram exatamente 10 s uma da outra, e a terceira falha foi para a DLQ.

### Caso 8: DLQ

**Entrada**
```sh
mq queues/%2Fshop/dlq | jq .messages
mq queues/%2Fshop/dlq/get -X POST -d '{"count":10,"ackmode":"ack_requeue_true","encoding":"auto"}' \
  | jq -c '.[0] | {routing_key, payload, x_death: [.properties.headers["x-death"][] | {queue, reason, count}]}'
```

**Processamento.** Na terceira falha, o `worker` lê o `x-death`, publica a mensagem
original no `dlx` com *confirm* e só então dá *ack*. O `get` com `ack_requeue_true` só
espia: a mensagem continua na fila.

**Saída**
```
1
{"routing_key":"reservation.created",
 "payload":"{\"id\":\"23a55896-…\",\"type\":\"reservation.created\",\"order_id\":\"921994ea-…\",\"data\":{}}",
 "x_death":[{"queue":"retry.q","reason":"expired","count":2},{"queue":"payment","reason":"rejected","count":2}]}
```
Duas rejeições no `payment` antes da terceira tentativa, e duas esperas na `retry.q`.

### Caso 9: `worker` parado e broker reiniciado

![Broker e consumidor indisponíveis](diagramas/sequencia-indisponivel.png)

**Entrada**
```sh
docker compose stop worker; stock 10; for i in 1 2 3; do order 1 >/dev/null; done; sleep 2
mq queues/%2Fshop/stock | jq .messages
mq queues/%2Fshop/stock/get -X POST -d '{"count":1,"ackmode":"ack_requeue_true","encoding":"auto"}' | jq -c '.[0].properties'
docker compose restart rabbitmq; sleep 15; mq queues/%2Fshop/stock | jq .messages
docker compose start worker; sleep 8; mq queues/%2Fshop/stock | jq .messages
```

**Processamento.** Sem consumidor, os `order.placed` ficam na fila durável. Como as
mensagens são persistentes, sobrevivem ao reinício do broker. O `worker` reconecta e
esvazia a fila.

**Saída**
```
stock com o worker parado: 3
{"routing_key":"order.placed","properties":{"delivery_mode":2,"content_type":"application/json","message_id":"a4ce54f6-…"}}
stock depois de reiniciar o rabbitmq: 3
stock depois de religar o worker: 0
```
As mensagens resistiram ao `restart` porque o contêiner continua o mesmo. Um
`docker compose down` apagaria as mensagens, porque o broker não tem volume de dados
(limitação na Etapa 5).

### Caso 10: Três réplicas

**Entrada**
```sh
docker compose up -d --scale worker=3; sleep 8
for q in stock payment notification; do echo "$q $(mq queues/%2Fshop/$q | jq .consumers)"; done
mq queues/%2Fshop/stock | jq '.consumer_details[0].prefetch_count'
stock 100; T=$(date -u +%Y-%m-%dT%H:%M:%SZ); for i in $(seq 30); do order 1 >/dev/null; done; sleep 6
docker compose logs --since "$T" worker | grep "stock: reserved" | awk '{print $1}' | sort | uniq -c
docker compose up -d --scale worker=1
```

**Processamento.** Cada réplica abre um consumidor por fila, e o broker entrega cada
mensagem a um só deles, no máximo 10 sem *ack* por consumidor.

**Saída**
```
stock consumers=3   payment consumers=3   notification consumers=3   prefetch=10
reservas por réplica (30 pedidos):  worker-1: 11   worker-2: 8   worker-3: 11
```

### Caso 11: Mensagem repetida

**Entrada**
```sh
stock 10; order 1; sleep 3
ID=$(docker compose logs worker | grep "queue=stock type=order.placed" | grep "order_id=$C" | head -1 | sed -E 's/.* id=([0-9a-f-]+).*/\1/')
echo "ID=$ID available antes=$(avail) status=$(st $C)"
pub order.placed "$(jq -nc --arg id "$ID" --arg o "$C" '{id:$id,type:"order.placed",order_id:$o,data:{}}')"
sleep 3; docker compose logs --no-log-prefix worker | grep "stock: duplicate id=$ID"; avail
docker compose logs worker | grep "notification: type=order.placed id=$ID" | wc -l
```

**Processamento.** O `stock` tenta gravar o `id` em `processed_messages`, encontra o
registro, anota `duplicate` e confirma sem fazer nada. O `notification` não deduplica,
então registra o evento duas vezes.

**Saída**
```
ID=fa9215e8-… available antes=9 status=PAID
{"routed":true}
18:15:43 stock: duplicate id=fa9215e8-…
available depois=9
linhas do notification para esse id: 2
```

### Caso 12: Broker fora no checkout

**Entrada**
```sh
stock 10; C=$(curl -s -X POST localhost:8080/carts | jq -r .id)
curl -s -X POST localhost:8080/carts/$C/items -d "{\"product_id\":\"$P\",\"quantity\":1}" >/dev/null
docker compose stop rabbitmq
curl -s -w ' HTTP %{http_code} %{time_total}s\n' -X POST localhost:8080/carts/$C/checkout; st $C
docker compose start rabbitmq
until curl -s -w ' HTTP %{http_code}\n' -X POST localhost:8080/carts/$C/checkout | grep 201; do sleep 2; done
sleep 5; st $C; avail
```

**Processamento.** A `api` muda o pedido para `PLACED` e tenta publicar. Sem broker, o
limite de 5 s estoura, a transação é desfeita e a resposta é `503`. O pedido continua
`CART`, e o cliente pode tentar de novo.

**Saída**
```
{"error":"broker unavailable: context deadline exceeded"} HTTP 503 5.005426s
status=CART available=10
{"id":"6583ae4b-…","status":"PLACED"} HTTP 201   (segunda tentativa, com o broker de volta)
status=PAID available=9
```

### Caso 13: Erros de entrada

**Entrada** (`$C` é um pedido já pago)
```sh
E=$(curl -s -X POST localhost:8080/carts | jq -r .id); Z=00000000-0000-0000-0000-000000000000
curl -s -w ' %{http_code}\n' -X POST localhost:8080/carts/$E/items -d "{\"product_id\":\"$P\",\"quantity\":0}"
curl -s -w ' %{http_code}\n' -X POST localhost:8080/carts/$E/items -d '{oops'
curl -s -w ' %{http_code}\n' -X POST localhost:8080/carts/$E/items -d "{\"product_id\":\"$Z\",\"quantity\":1}"
curl -s -w ' %{http_code}\n' localhost:8080/orders/abc
curl -s -w ' %{http_code}\n' localhost:8080/orders/$Z
curl -s -w ' %{http_code}\n' -X POST localhost:8080/carts/$E/checkout
curl -s -w ' %{http_code}\n' -X POST localhost:8080/carts/$C/checkout
```

**Processamento.** A `api` valida tudo antes de publicar qualquer coisa.

**Saída**
```
{"error":"bad request: need product_id and quantity > 0"} 400
{"error":"bad request: need product_id and quantity > 0"} 400
{"error":"not found: product 00000000-0000-0000-0000-000000000000"} 404
{"error":"not found: \"abc\""} 404
{"error":"not found: order 00000000-0000-0000-0000-000000000000"} 404
{"error":"conflict: cart is empty"} 409
{"error":"conflict: order is PAID, not CART"} 409
```

### Caso 14: Mensagem inválida

**Entrada**
```sh
mq queues/%2Fshop/dlq | jq .messages
pub order.placed '{"id":"not-a-uuid","type":"order.placed","order_id":"x","data":{}}'
pub order.placed 'isto não é json'
sleep 3; docker compose logs --no-log-prefix worker | grep -- "-> dlq" | tail -4
docker compose logs worker | grep 'id=not-a-uuid' | grep -c attempt=
mq queues/%2Fshop/dlq | jq .messages
```

**Processamento.** As duas mensagens chegam ao `stock` e ao `notification`. A leitura
rejeita a primeira (o `id` não é UUID) e a segunda (não é JSON), e cada consumidor manda
a mensagem direto para a DLQ, sem passar pela retentativa.

**Saída**
```
18:15:46 queue=notification id= -> dlq: id: invalid uuid
18:15:46 queue=stock id= -> dlq: id: invalid uuid
18:15:46 queue=notification id= -> dlq: invalid character 'i' looking for beginning of value
18:15:46 queue=stock id= -> dlq: invalid character 'i' looking for beginning of value
tentativas registradas: 0    dlq: 0 → 4
```
O `id=` aparece vazio porque a publicação manual não preencheu o `message_id`.

### Caso 15: `worker` morto no meio do processamento

**Entrada**
```sh
stock 1000; : > /tmp/batch.ids
for i in $(seq 200); do c=$(curl -s -X POST localhost:8080/carts | jq -r .id)
  curl -s -o /dev/null -X POST localhost:8080/carts/$c/items -d "{\"product_id\":\"$P\",\"quantity\":1}"; echo $c >> /tmp/batch.ids; done
T=$(date -u +%Y-%m-%dT%H:%M:%SZ)
for c in $(cat /tmp/batch.ids); do curl -s -o /dev/null -X POST localhost:8080/carts/$c/checkout & done
until docker compose logs --since "$T" worker | grep -q "stock: reserved"; do :; done
docker compose kill -s SIGKILL worker; wait
IDS=$(sed "s/.*/'&'/" /tmp/batch.ids | paste -sd, -)
sql "SELECT status, count(*) FROM orders WHERE id IN ($IDS) GROUP BY 1"
docker compose start worker; sleep 20
sql "SELECT status, count(*) FROM orders WHERE id IN ($IDS) GROUP BY 1"
sql "SELECT sum(i.quantity) FROM order_items i JOIN orders o ON o.id=i.order_id WHERE o.id IN ($IDS) AND o.status IN ('RESERVED','PAID')"; avail
docker compose logs --since "$T" worker | grep -E ": duplicate|refund simulated"
```

**Processamento.** As mensagens entregues e ainda sem *ack* voltam para a fila, e as
transações abertas são desfeitas pelo PostgreSQL. Se o processo morreu depois de
publicar e antes do `COMMIT`, o evento já publicado encontra o status antigo e é
ignorado; a mensagem original volta e é processada de novo.

**Saída**
```
logo após o kill:     PLACED 148  RESERVED 3  PAID 46  DECLINED 3   (stock: 151 na fila)
após religar:         PAID 172  DECLINED 28
unidades vendidas: 172   available: 828   (1000 − 828 = 172)
18:22:36 payment: refund simulated order_id=… amount_cents=24990
```
Todos os 200 pedidos terminaram, e o estoque fechou exatamente. O `refund simulated` é
o caso do intervalo entre publicar e fazer `COMMIT`: o `payment` recebeu a reserva de um
pedido cuja transação foi desfeita, não cobrou nada, e o pedido foi reservado e pago
normalmente depois.

Na primeira execução deste caso, 42 checkouts falharam com
`FATAL: sorry, too many clients already`: o pool de conexões do Go não tinha limite.
Corrigimos com `SetMaxOpenConns(20)` (commit `e4e1e5a`), e a saída acima é da execução
corrigida.

### Caso 16: Pagamento depois da expiração

**Entrada** (o pedido `EXPIRED` do caso 5 e o pedido pago do caso 1)
```sh
FAIL_RATE=0 docker compose up -d worker; sleep 4
# repetir até aparecer "refund simulated" (15 % das tentativas são recusadas)
pub reservation.created "$(jq -nc --arg id "$(uuidgen | tr A-Z a-z)" --arg o "$C5" '{id:$id,type:"reservation.created",order_id:$o,data:{}}')"
sleep 2; docker compose logs --no-log-prefix worker | grep "order_id=$C5" | grep -E "payment: (refund|declined)|stock: noop"
st $C5; avail
docker compose logs --no-log-prefix worker | grep "order_id=$C1" | grep -E "type=reservation.expired|stock: noop"
```

**Processamento.** O `payment` tenta mudar o pedido de `RESERVED` para `PAID`, mas ele
está `EXPIRED`. O *compare-and-set* falha, nada é publicado e o estorno é simulado. No
caso 1, a expiração chega a um pedido `PAID` e também não muda nada.

**Saída**
```
18:19:09 payment: declined order_id=921994ea-…     (primeira tentativa sorteou recusa)
18:19:09 stock: noop order_id=921994ea-…
18:19:11 payment: refund simulated order_id=921994ea-… amount_cents=24990
status=EXPIRED available=10

18:21:43 queue=stock type=reservation.expired order_id=d93c8d8e-…   (pedido do caso 1)
18:21:43 stock: noop order_id=d93c8d8e-…
status=PAID
```

### Caso 17: Segurança

![Usuários, permissões e TLS](diagramas/seguranca.png)

**Entrada**
```sh
echo | openssl s_client -connect localhost:5671 -CAfile certs/ca.pem
for p in 5672 15672; do nc -z -w2 localhost $p && echo "$p aberta" || echo "$p fechada"; done
docker compose exec -T rabbitmq rabbitmqctl -q list_permissions -p /shop
curl -s -o /dev/null -w '%{http_code}\n' --cacert certs/ca.pem -u guest:guest https://localhost:15671/api/overview
curl -s -o /dev/null -w '%{http_code}\n' --cacert certs/ca.pem -u api:$API_PASS https://localhost:15671/api/overview
curl -s --cacert certs/ca.pem -u monitor:$MONITOR_PASS -X POST https://localhost:15671/api/exchanges/%2Fshop/orders/publish \
  -d '{"properties":{},"routing_key":"order.placed","payload":"{}","payload_encoding":"string"}'
curl -s --cacert certs/ca.pem -u monitor:$MONITOR_PASS -X DELETE https://localhost:15671/api/queues/%2Fshop/stock/contents
git -C .. ls-files deploy
```

**Processamento.** O broker só tem portas TLS, e cada usuário só alcança o que sua
permissão libera (Etapa 3, §7 e §8).

**Saída**
```
subject=CN=rabbitmq   issuer=CN=shop-ca
New, TLSv1.3, Cipher is TLS_AES_256_GCM_SHA384
Verify return code: 0 (ok)
5672 fechada   15672 fechada
api      ^$  ^orders$               ^$
worker   ^$  ^(orders|expiry|dlx)$  ^(stock|payment|notification)$
monitor  ^$  ^$                     ^dlq$
admin    .*  .*                     .*
guest: 401   api na Management UI: 401
monitor publicando: 403 ACCESS_REFUSED - write access to exchange 'orders' refused
monitor esvaziando a fila stock: 401
arquivos versionados: definitions.tmpl.json, docker-compose.yml, init.sql, rabbitmq.conf, setup.sh
```
Com um cliente AMQP de teste, `api` publicando no `retry`, `dlx` ou `expiry` e `worker`
publicando no `retry` também receberam `ACCESS_REFUSED`. Nenhum arquivo com senha ou
certificado está no git.
