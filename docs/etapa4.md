# message-queue-ecommerce
## Etapa 4 — Casos de Uso

**Trabalho Prático — Mensageria** · Sistemas Distribuídos · FURB
Prof. Gabriel Castellani · Entrega: a definir

---

## Ambiente

| Item | Valor |
|---|---|
| Máquina | macOS 26.6.2, `arm64` |
| Docker | 29.4.2, Docker Compose v5.1.3 |
| Commit | `0d7898c` |
| Execuções | **A** — 30/09/2026 03:23 UTC (casos 1, 2, 3, 4, 6, 10, 11 e US8) · **B** — 30/09/2026 15:51 UTC (casos 5, 7, 8 e 9) |

Cada execução começa do zero (`docker compose down -v && docker compose up -d --build`).
A execução B repete os casos 5 e 9: na A, a máquina hospedeira entrou em
suspensão durante a espera de 120 s, o que congelou os temporizadores de TTL
do RabbitMQ (as expirações só ocorreram após o fim do roteiro); a B rodou com
`caffeinate` para impedir a suspensão.

Comandos executados a partir de `deploy/`, com as funções auxiliares:

```sh
sql() { docker compose exec -T postgres psql -U shop -d shop -tAc "$1"; }
mq()  { curl -s -u worker:worker "localhost:15672/api/$1" "${@:2}"; }
P=$(sql "SELECT id FROM products WHERE name='Keyboard'")
order() { C=$(curl -s -X POST localhost:8080/carts | jq -r .id)
  curl -s -X POST localhost:8080/carts/$C/items -d "{\"product_id\":\"$P\",\"quantity\":${1:-1}}" >/dev/null
  curl -s -X POST localhost:8080/carts/$C/checkout; echo; }
st()    { curl -s localhost:8080/orders/$1 | jq -r .status; }
stock() { sql "UPDATE products SET available=$1 WHERE id='$P'"; }
avail() { sql "SELECT available FROM products WHERE id='$P'"; }
```

As saídas abaixo são reais, recortadas nas linhas relevantes (o prefixo de
data/hora dos logs foi mantido).

---

### Caso 1 — Fluxo feliz

- **Objetivo:** US1 (checkout imediato) e US3 (estoque e pagamento corretos).
- **Entrada:**
  ```sh
  stock 10; order 1; sleep 3; st $C; avail
  docker compose logs worker | grep "notification: .*order_id=$C"
  ```
  Tempo de resposta com o `worker` parado:
  ```sh
  docker compose stop worker
  C=$(curl -s -X POST localhost:8080/carts | jq -r .id)
  curl -s -X POST localhost:8080/carts/$C/items -d "{\"product_id\":\"$P\",\"quantity\":1}" >/dev/null
  curl -s -o /dev/null -w '%{http_code} %{time_total}\n' -X POST localhost:8080/carts/$C/checkout
  st $C; docker compose start worker; sleep 5; st $C
  ```
- **Processamento:** `api` → `order.placed` (`orders`) → `stock` reserva e publica `reservation.created` → `payment` aprova e publica `payment.approved`; `notification` recebe os três.
- **Saída esperada:** status `PAID`; `available` cai 1; eventos na ordem `order.placed` → `reservation.created` → `payment.approved`; checkout em < 0,3 s mesmo sem `worker`.
- **Saída obtida (execução A):**
  ```
  {"id":"a7d0b682-9f81-4a6c-a208-bcbbbee1297d","status":"PLACED"}
  status=PAID
  available=9
  {"id":"a7d0b682-...","items":[{"product_id":"34aba412-...","quantity":1,"price_cents":24990}],"status":"PAID","total_cents":24990}
  2026/09/30 03:23:38 notification: type=order.placed id=17154f79-... order_id=a7d0b682-...
  2026/09/30 03:23:38 notification: type=reservation.created id=b2a6b0d5-... order_id=a7d0b682-...
  2026/09/30 03:23:38 notification: type=payment.approved id=d11f93df-... order_id=a7d0b682-...
  2026/09/30 03:23:38 payment: approved order_id=a7d0b682-... transaction_id=TX-5131a4ab amount_cents=24990

  201 0.002897
  status (worker parado)=PLACED
  status (worker iniciado)=DECLINED
  ```
- **Resultado:** ok. Checkout respondeu `201` em 2,9 ms com o `worker` parado; o pedido ficou `PLACED` e foi processado quando o `worker` voltou (terminou `DECLINED`, a recusa simulada de ~15 %, também um desfecho válido).

### Caso 2 — Pagamento recusado

- **Objetivo:** US3 (recusa devolve o estoque).
- **Entrada:**
  ```sh
  stock 100; for i in $(seq 20); do order 1 >/dev/null; done; sleep 8
  sql "SELECT status, count(*) FROM orders WHERE status IN ('PAID','DECLINED') GROUP BY 1 ORDER BY 1"
  docker compose logs worker | grep -E "payment: declined order_id=$D|stock: released order_id=$D"; avail
  ```
- **Processamento:** `payment` sorteia a recusa (~15 %) e publica `payment.declined`; `stock` faz o compare-and-set `RESERVED → DECLINED` e devolve as unidades.
- **Saída esperada:** pedidos `DECLINED` com o estoque devolvido.
- **Saída obtida (execução A):**
  ```
  DECLINED|2
  PAID|20
  declined order: 455dc78b-23b7-4e79-a636-54cae236772f
  2026/09/30 03:23:42 payment: declined order_id=455dc78b-...
  2026/09/30 03:23:42 stock: released order_id=455dc78b-... status=DECLINED
  available=81
  ```
- **Resultado:** ok. A contagem inclui os dois pedidos do caso 1 (um `PAID`, um `DECLINED`); dos 20 deste caso, 19 foram pagos e 1 recusado, e `available = 100 − 19 = 81` confirma que a unidade recusada voltou.

### Caso 3 — Estoque insuficiente

- **Objetivo:** US3 (sem estoque, sem cobrança).
- **Entrada:**
  ```sh
  stock 10; order 11; sleep 3; st $C
  docker compose logs worker | grep "order_id=$C" | grep -c queue=payment; avail
  ```
- **Processamento:** a reserva falha no `UPDATE ... AND available >= $1`; a transação é desfeita, o pedido vai para `OUT_OF_STOCK` e `stock` publica `reservation.rejected` (nenhum `reservation.created`).
- **Saída esperada:** `OUT_OF_STOCK`; `payment` nunca recebe mensagem.
- **Saída obtida (execução A):**
  ```
  {"id":"0368a2f2-f8bd-46c1-8f21-102f04d9e984","status":"PLACED"}
  status=OUT_OF_STOCK
  payment lines for order: 0
  2026/09/30 03:23:56 stock: out_of_stock order_id=0368a2f2-...
  available=10
  ```
- **Resultado:** ok.

### Caso 4 — Última unidade, dois clientes

- **Objetivo:** US9 (sem venda acima do estoque).
- **Entrada:**
  ```sh
  stock 1
  C1=$(curl -s -X POST localhost:8080/carts | jq -r .id); C2=$(curl -s -X POST localhost:8080/carts | jq -r .id)
  for c in $C1 $C2; do curl -s -X POST localhost:8080/carts/$c/items -d "{\"product_id\":\"$P\",\"quantity\":1}" >/dev/null; done
  curl -s -XPOST localhost:8080/carts/$C1/checkout & curl -s -XPOST localhost:8080/carts/$C2/checkout & wait; sleep 4
  st $C1; st $C2; avail
  ```
- **Processamento:** os dois `order.placed` disputam o mesmo `UPDATE` atômico; só um encontra `available >= 1`.
- **Saída esperada:** um pedido reserva (e termina `PAID` ou `DECLINED`), o outro fica `OUT_OF_STOCK`; `available` nunca negativo — `0` se o vencedor pagou, `1` se foi recusado.
- **Saída obtida (execução A):**
  ```
  {"id":"9bc18c8f-b22b-404e-a6e0-37332eb6dbe3","status":"PLACED"}
  {"id":"b3cbebce-1309-404b-b111-83e233b0c8e1","status":"PLACED"}
  C1=OUT_OF_STOCK C2=PAID available=0
  2026/09/30 03:23:59 stock: reserved order_id=9bc18c8f-...
  2026/09/30 03:23:59 stock: out_of_stock order_id=b3cbebce-...
  ```
- **Resultado:** ok. Um vencedor (`PAID`), um `OUT_OF_STOCK`, `available = 0`. (Os ids das respostas saíram na ordem em que os dois `curl` terminaram, não na ordem `C1`, `C2`.)

### Caso 5 — Expiração da reserva

- **Objetivo:** US10 (reserva abandonada devolve o estoque).
- **Entrada:**
  ```sh
  stock 10; FAIL_RATE=1 docker compose up -d worker; order 1; sleep 2; st $C; avail
  # aguardar ≥ 130 s a partir do checkout
  st $C; avail
  docker compose logs worker | grep "order_id=$C" | grep -E "stock: (reserved|released)"
  ```
- **Processamento:** `stock` reserva e publica `reservation.expired` no exchange `expiry`; `expiry.q` o segura por 120 s (`x-message-ttl: 120000`) e o devolve ao `orders`; como o pagamento nunca aprova (casos 7 e 8), `stock` faz `RESERVED → EXPIRED` e devolve a unidade.
- **Saída esperada:** pagamento falha 3 vezes e vai para a `dlq`; pedido fica `RESERVED`; após 120 s, `EXPIRED` e estoque restaurado.
- **Saída obtida (execução B):**
  ```
  {"id":"8995b8f6-b694-4e21-9ce7-6ce1ee167f57","status":"PLACED"}
  status=RESERVED available=9
  status=RESERVED  (30 s após o checkout)
  status=EXPIRED available=10  (136 s após o checkout)
  2026/09/30 15:51:36 stock: reserved order_id=8995b8f6-...
  2026/09/30 15:53:36 stock: released order_id=8995b8f6-... status=EXPIRED
  ```
- **Resultado:** ok. Reserva às 15:51:36, liberação às 15:53:36 — exatamente 120 s. As 3 tentativas e a DLQ estão nos casos 7 e 8 (mesma execução).

### Caso 6 — Fan-out

- **Objetivo:** US4 (um evento, vários consumidores).
- **Entrada:** `docker compose logs worker | grep "type=reservation.created" | grep "order_id=<pedido do caso 1>" | grep "queue="`
- **Processamento:** `reservation.created` é publicado uma vez no `orders`; o binding `reservation.created` entrega em `payment` e o binding `#` entrega em `notification`.
- **Saída esperada:** a mesma mensagem nos logs de `payment` e `notification`.
- **Saída obtida (execução A):**
  ```
  2026/09/30 03:23:38 queue=notification type=reservation.created id=b2a6b0d5-ecb0-40d6-a64d-6a70101eeffe order_id=a7d0b682-... attempt=1
  2026/09/30 03:23:38 queue=payment type=reservation.created id=b2a6b0d5-ecb0-40d6-a64d-6a70101eeffe order_id=a7d0b682-... attempt=1
  ```
- **Resultado:** ok. Mesmo `id` nas duas filas.

### Caso 7 — Retentativa

- **Objetivo:** US5 (falha transitória é retentada).
- **Entrada:** mesma execução do caso 5 (`FAIL_RATE=1`); após ~25 s:
  ```sh
  docker compose logs worker | grep "order_id=$C" | grep -E "queue=payment type=reservation.created|payment: gateway error"
  docker compose logs worker | grep "queue=payment" | grep -- "-> dlq"
  ```
- **Processamento:** `payment` falha (erro simulado do gateway) → `Nack` sem requeue → `retry` → `retry.q` (10 s) → volta ao `orders`. São 3 tentativas no total.
- **Saída esperada:** a mesma mensagem reprocessada a cada 10 s.
- **Saída obtida (execução B):**
  ```
  2026/09/30 15:51:36 queue=payment type=reservation.created id=ffeead7f-... order_id=8995b8f6-... attempt=1
  2026/09/30 15:51:36 payment: gateway error (FAIL_RATE) order_id=8995b8f6-...
  2026/09/30 15:51:46 queue=payment type=reservation.created id=ffeead7f-... order_id=8995b8f6-... attempt=2
  2026/09/30 15:51:46 payment: gateway error (FAIL_RATE) order_id=8995b8f6-...
  2026/09/30 15:51:56 queue=payment type=reservation.created id=ffeead7f-... order_id=8995b8f6-... attempt=3
  2026/09/30 15:51:56 payment: gateway error (FAIL_RATE) order_id=8995b8f6-...
  2026/09/30 15:51:56 queue=payment id=ffeead7f-... -> dlq: simulated gateway error
  ```
- **Resultado:** ok. Tentativas 1, 2 e 3 exatamente 10 s uma da outra; a 3ª falha vai para a DLQ.

### Caso 8 — DLQ

- **Objetivo:** US5 (mensagem que esgota as tentativas fica isolada e legível).
- **Entrada:**
  ```sh
  mq queues/%2Fshop/dlq | jq .messages
  mq queues/%2Fshop/dlq/get -X POST -d '{"count":10,"ackmode":"ack_requeue_true","encoding":"auto"}' | jq '...'
  ```
- **Processamento:** na 3ª falha, o `worker` republica a mensagem original no `dlx` (com confirm) e só então dá ack; `dlx` entrega na `dlq`.
- **Saída esperada:** mensagem visível na `dlq`, com o histórico `x-death`.
- **Saída obtida (execução B):**
  ```
  dlq messages=1
  {"routing_key":"reservation.created",
   "payload":"{\"id\":\"ffeead7f-...\",\"type\":\"reservation.created\",\"order_id\":\"8995b8f6-...\",\"data\":{}}",
   "x_death":[{"queue":"retry.q","reason":"expired","count":2},{"queue":"payment","reason":"rejected","count":2}]}
  ```
- **Resultado:** ok. `payment`/`rejected` com `count: 2` (duas rejeições antes da 3ª tentativa, que foi para a DLQ); as entradas `retry.q`/`expired` são as esperas de 10 s.

### Caso 9 — Consumidor fora do ar

- **Objetivo:** US2 (nenhum pedido se perde com o `worker` ou o broker fora).
- **Entrada:**
  ```sh
  docker compose stop worker; stock 10; for i in 1 2 3; do order 1 >/dev/null; done; sleep 12
  mq queues/%2Fshop/stock | jq .messages
  mq queues/%2Fshop/stock/get -X POST -d '{"count":1,"ackmode":"ack_requeue_true","encoding":"auto"}' | jq '...'
  docker compose restart rabbitmq; sleep 15; mq queues/%2Fshop/stock | jq .messages
  docker compose start worker; sleep 8; mq queues/%2Fshop/stock | jq .messages
  ```
- **Processamento:** sem consumidor, os `order.placed` acumulam na fila durável `stock`; as mensagens são persistentes (`delivery_mode: 2`) e sobrevivem ao reinício do broker; o `worker` reconecta e esvazia a fila.
- **Saída esperada:** fila acumula e depois esvazia.
- **Saída obtida (execução B):**
  ```
  stock messages (worker parado)=3
  {"routing_key":"order.placed","properties":{"delivery_mode":2,"content_type":"application/json","message_id":"522c9545-..."},
   "payload":"{\"id\":\"522c9545-...\",\"type\":\"order.placed\",\"order_id\":\"a5a944fd-...\",\"data\":{}}"}
  stock messages (após reiniciar o rabbitmq)=3
  stock messages (worker iniciado)=0
  ```
- **Resultado:** ok.

### Caso 10 — Escalabilidade

- **Objetivo:** US6 (consumidores concorrentes).
- **Entrada:**
  ```sh
  docker compose up -d --scale worker=3; sleep 8
  for q in stock payment notification; do mq queues/%2Fshop/$q | jq .consumers; done
  mq queues/%2Fshop/stock | jq '.consumer_details[0].prefetch_count'
  ```
- **Processamento:** cada réplica do `worker` abre um consumidor por fila; o RabbitMQ distribui as mensagens entre eles, limitado por `prefetch = 10`.
- **Saída esperada:** 3 consumidores por fila.
- **Saída obtida (execução A):**
  ```
  stock consumers=3
  payment consumers=3
  notification consumers=3
  prefetch=10
  ```
- **Resultado:** ok.

### Caso 11 — Idempotência

- **Objetivo:** US7 (mensagem repetida não tem efeito duplicado).
- **Entrada:**
  ```sh
  stock 10; order 1; sleep 3
  ID=$(docker compose logs worker | grep "queue=stock type=order.placed" | grep "order_id=$C" | head -1 | sed -E 's/.* id=([0-9a-f-]+).*/\1/')
  avail
  BODY=$(jq -nc --arg id "$ID" --arg o "$C" '{id:$id,type:"order.placed",order_id:$o,data:{}}')
  mq exchanges/%2Fshop/orders/publish -X POST -H 'content-type: application/json' \
    -d "$(jq -nc --arg p "$BODY" '{properties:{delivery_mode:2,content_type:"application/json"},routing_key:"order.placed",payload:$p,payload_encoding:"string"}')"
  sleep 3; docker compose logs worker | grep "stock: duplicate id=$ID"; avail
  ```
- **Processamento:** o `id` reenviado já está em `processed_messages`; `store.MarkProcessed` retorna falso e o `stock` só registra `duplicate`.
- **Saída esperada:** `available` cai uma única vez.
- **Saída obtida (execução A):**
  ```
  {"id":"4826fe6c-120f-4489-938c-22304a734c70","status":"PLACED"}
  ID=234a7abc-9b5c-4a44-b070-ba185dda50b6 available antes=9 status=PAID
  {"routed":true}
  2026/09/30 03:24:07 stock: duplicate id=234a7abc-9b5c-4a44-b070-ba185dda50b6
  available depois=9
  ```
- **Resultado:** ok.

### US8 — Filas visíveis na Management UI

- **Entrada:** `mq queues/%2Fshop | jq -r '.[].name'` (a mesma listagem da aba *Queues* em `http://localhost:15672`, login `worker`).
- **Saída obtida (execução A):**
  ```
  dlq
  expiry.q
  notification
  payment
  retry.q
  stock
  ```
- **Resultado:** ok. As 6 filas aparecem, e o conteúdo da `dlq` é legível (caso 8).

### Parada graciosa

- **Entrada:** `docker compose stop api worker; docker compose ps -a; docker compose logs worker | tail -1`
- **Saída obtida (execução B):**
  ```
  api Exited (0)
  worker Exited (0)
  2026/09/30 15:53:52 worker: stopped
  ```
- **Resultado:** ok. Os dois processos terminam com código 0 ao receber `SIGTERM`.

## Cobertura

| Caso | História | Resultado |
|---|---|---|
| 1 — Fluxo feliz | US1, US3 | ok |
| 2 — Pagamento recusado | US3 | ok |
| 3 — Estoque insuficiente | US3 | ok |
| 4 — Última unidade | US9 | ok |
| 5 — Expiração da reserva | US10 | ok |
| 6 — Fan-out | US4 | ok |
| 7 — Retentativa | US5 | ok |
| 8 — DLQ | US5 | ok |
| 9 — Consumidor fora do ar | US2 | ok |
| 10 — Escalabilidade | US6 | ok |
| 11 — Idempotência | US7 | ok |
| Filas na Management UI | US8 | ok |
