# message-queue-ecommerce

Trabalho prático da disciplina de Sistemas Distribuídos. O projeto demonstra o uso de mensageria (RabbitMQ) no backend de processamento de pedidos de uma loja virtual.

## Cenário

O sistema processa pedidos de e-commerce em quatro etapas: registrar o pedido (o carrinho vira pedido no checkout), reservar temporariamente o estoque, cobrar o pagamento e notificar o cliente. O estoque é reservado antes da cobrança para que nenhum cliente pague por um produto que acabou; se o pagamento não se concluir no prazo, a reserva expira. Essas etapas têm latências e dependências externas muito diferentes, o que motiva desacoplá-las com um broker de mensagens em vez de executá-las de forma síncrona.

Detalhes da justificativa em [`docs/etapa1.md`](docs/etapa1.md).

## Arquitetura

Dois processos (`api` e `worker`), um broker RabbitMQ e um banco PostgreSQL. No checkout, a API publica o evento `order.placed` em um exchange do tipo `topic`; o handler `stock` reserva as unidades e publica `reservation.created`, que o handler `payment` consome para cobrar. Os handlers do `worker` (estoque, pagamento, notificação) encadeiam o fluxo sem se conhecerem diretamente.

Inclui estratégias de escalabilidade (competing consumers), confiabilidade (publisher confirms, ack manual, idempotência) e tolerância a falhas (retry com TTL, dead letter queue, expiração da reserva com TTL + dead lettering).

Detalhes completos, diagramas e justificativa de decisões em [`docs/etapa2.md`](docs/etapa2.md).

## Documentação

| Arquivo | Conteúdo |
|---|---|
| [`docs/PRD.md`](docs/PRD.md) | Especificação de build: nomes, casos de uso, regras de implementação, plano de entrega |
| [`docs/etapa1.md`](docs/etapa1.md) | Cenário e justificativa da mensageria |
| [`docs/etapa2.md`](docs/etapa2.md) | Arquitetura, topologia, escalabilidade, confiabilidade, tolerância a falhas |
| [`docs/etapa3.md`](docs/etapa3.md) | Configuração do RabbitMQ: exchanges, filas, argumentos, usuários, TLS |
| [`docs/etapa4.md`](docs/etapa4.md) | Execução dos casos de uso, com comandos e evidências |
| [`docs/etapa5.md`](docs/etapa5.md) | Stack, estrutura do código, formato das mensagens, boas práticas, limitações |
| [`docs/diagramas/`](docs/diagramas) | Diagramas (`.mmd` e `.png`) |
| [`internal/api/openapi.yaml`](internal/api/openapi.yaml) | Contrato da API HTTP (OpenAPI 3.1), servido em `GET /openapi.yaml` |

## Como executar

### 1. Pré-requisitos

Docker com Compose v2, `openssl`, `curl` e `jq`. Go 1.27 só é necessário para compilar localmente (`go build ./...`); o Docker compila a imagem sozinho.

### 2. Subir o ambiente

```sh
cd deploy
sh setup.sh
docker compose up -d --build
docker compose ps
until curl -s localhost:8080/orders/x >/dev/null; do sleep 1; done
```

`setup.sh` (uma vez) gera o que fica fora do git: CA e certificado TLS do broker (`certs/`), senhas aleatórias dos 4 usuários do RabbitMQ (`.env`), o `definitions.json` a partir de `definitions.tmpl.json` e o `web/.env.local`. `docker compose ps` deve mostrar os 4 serviços rodando: `rabbitmq`, `postgres`, `api` e `worker`. O `until` espera a API responder — ela só aceita requisições depois que o banco e o broker estão prontos (leva ~15 s na primeira vez). Todos os comandos seguintes são executados a partir de `deploy/`. Se o `init.sql` mudou desde o último `up`, recrie o banco com `docker compose up -d --force-recreate -V postgres` (o `-V` descarta o volume anônimo do Postgres; os dados recomeçam do zero).

### 3. Management UI do RabbitMQ

Acesse `https://localhost:15671` com usuário `admin` e a senha `ADMIN_PASS` de `deploy/.env`. O navegador alerta sobre o certificado (CA própria, `deploy/certs/ca.pem`). Só há listeners TLS: AMQP em `5671` e HTTPS em `15671`; as portas `5672`/`15672` não existem e o usuário `guest` não existe.

### 4. Funções auxiliares

Cole no terminal (bash ou zsh), dentro de `deploy/`:

```sh
. ./.env
sql() { docker compose exec -T postgres psql -U shop -d shop -tAc "$1"; }
mq()  { curl -s --cacert certs/ca.pem -u admin:$ADMIN_PASS "https://localhost:15671/api/$1" "${@:2}"; }
P=$(sql "SELECT id FROM products WHERE name='Keyboard'")
```

`P` é o id do produto de exemplo (Keyboard, R$ 249,90, 10 unidades).

### 5. Fluxo básico

```sh
C=$(curl -s -X POST localhost:8080/carts | jq -r .id)
curl -s -X POST localhost:8080/carts/$C/items -d "{\"product_id\":\"$P\",\"quantity\":1}"
curl -s -X POST localhost:8080/carts/$C/checkout
sleep 2; curl -s localhost:8080/orders/$C | jq
```

O `status` final é `PAID` (ou `DECLINED` em ~15% das vezes, recusa simulada do pagamento). Para acompanhar o processamento: `docker compose logs -f worker`.

### 6. Falha, DLQ e expiração

Pela página (passo 9): *Pagamento falha 1×* mostra retry e recuperação; *Pagamento sempre falha* mostra 3 tentativas → `dlq` → `EXPIRED`, só naquele pedido. Pelo terminal, o equivalente na API é `checkout -d '{"simulate":"payment_once"}'` (ou `payment_always`). Para fazer **todos** os pagamentos falharem:

```sh
FAIL_RATE=1 docker compose up -d worker
```

Repita o passo 5: o pagamento falha 3 vezes (com ~10 s entre as tentativas) e a mensagem vai para a fila `dlq` (`mq queues/%2Fshop/dlq | jq .messages`). O pedido fica `RESERVED` e, após ~120 s, passa a `EXPIRED`, devolvendo a unidade ao estoque. Para voltar ao normal:

```sh
FAIL_RATE=0 docker compose up -d worker
```

### 7. Escalar os consumidores

```sh
docker compose up -d --scale worker=3
```

Na Management UI (aba *Queues*), cada fila passa a ter 3 consumidores.

### 8. Parar e limpar

```sh
docker compose down -v
```

O `-v` também apaga os dados do banco.

### 9. Mapa de Mensagens (interface web)

Uma página que desenha a topologia como um mapa de linhas e anima as mensagens ao vivo. Requer Node.js ≥ 20.9 (≥ 23 para `npm test`) e o ambiente do passo 2 rodando (o `setup.sh` já criou `web/.env.local`).

```sh
cd web
npm install
npm run dev
```

Abra `http://localhost:3000`. Os botões *Novo pedido*, *Rajada ×5* e *Sem estoque* usam a API real; a lista *Pedidos* mostra o status de cada um até o final; a seta ▸ abre a linha do tempo do pedido (cada entrega do worker: fila, tentativa, ack/retry/dlq e o motivo), com a contagem até o próximo TTL (`retry.q` 10 s, `expiry.q` 120 s). Clique na fila `dlq` para ver as mensagens que estão nela (sem removê-las).

Os botões *Falhas* fazem o pagamento daquele pedido falhar uma vez ou sempre (passo 6), e a linha já abre com a linha do tempo. A parada do worker, a escala e o `FAIL_RATE` global continuam no terminal (passos 6 e 7) e aparecem no mapa. Para repor o estoque, use os botões `+5` e `Repor 10` na seção Estoque da barra lateral.

Cada bolinha no mapa é uma mensagem **contada** pelos contadores da Management API do RabbitMQ (atualizados a cada 1 s), não uma mensagem identificada. A página é só leitura sobre o broker: as credenciais da Management API ficam no servidor do Next (`MQ_USER`/`MQ_PASS` em `web/.env.local`: usuário `monitor`, sem escrita e com `read` só na `dlq`); o `npm run dev` confia na CA via `NODE_EXTRA_CA_CERTS`.

### API HTTP (OpenAPI)

O contrato da API está em [`internal/api/openapi.yaml`](internal/api/openapi.yaml) (OpenAPI 3.1): rotas, corpos,
respostas de sucesso e de erro, com exemplos. O arquivo é embutido no binário e servido pela própria `api`:

```sh
curl -s localhost:8080/openapi.yaml
```

| Método | Rota | O que faz |
|---|---|---|
| `POST` | `/carts` | Cria um carrinho (pedido em `CART`) |
| `POST` | `/carts/{id}/items` | Adiciona ou substitui um item (`product_id`, `quantity`); o preço é congelado |
| `POST` | `/carts/{id}/checkout` | `CART → PLACED` e publica `order.placed`; `201` só após o *confirm* do broker, `503` se não vier. Corpo opcional `{"simulate":"payment_once"\|"payment_always"}` |
| `GET` | `/orders/{id}` | Status, itens, total e a linha do tempo de entregas ao `worker` |
| `GET` | `/products` | Produtos e estoque disponível |
| `POST` | `/products/{id}/stock` | Repõe estoque (`{"add": 1..1000}`) |
| `GET` | `/openapi.yaml` | Esta especificação |

Para ver a documentação navegável, abra o arquivo no [Swagger Editor](https://editor.swagger.io) (*File → Import
file*) ou gere uma página HTML com `npx @redocly/cli build-docs internal/api/openapi.yaml -o api.html && open api.html`. Para validar depois de mudar uma rota:
`npx @redocly/cli lint internal/api/openapi.yaml`.

### Casos de uso

Os 17 casos de uso, com comandos e evidências, estão em [`docs/etapa4.md`](docs/etapa4.md).

### Variáveis de ambiente

| Variável | Valor no Compose | Usada por |
|---|---|---|
| `AMQP_URL` | `amqps://api:${API_PASS}@rabbitmq:5671/%2Fshop` (api) · `amqps://worker:${WORKER_PASS}@rabbitmq:5671/%2Fshop` (worker) | `api`, `worker` |
| `SSL_CERT_FILE` | `/certs/ca.pem` | `api`, `worker` — CA usada pelo Go para validar o TLS do broker |
| `DB_URL` | `postgres://shop:shop@postgres:5432/shop?sslmode=disable` | `api`, `worker` |
| `FAIL_RATE` | `0` (padrão); `0` a `1` | `worker` — só afeta o pagamento (erro simulado do gateway) |

### Portas

| Porta | Serviço |
|---|---|
| `8080` | API HTTP (`api`); contrato em `/openapi.yaml` |
| `5671` | AMQP sobre TLS (RabbitMQ, só `127.0.0.1`) |
| `15671` | Management UI HTTPS (RabbitMQ, só `127.0.0.1`) |
| `3000` | Mapa de Mensagens (`web/`, `npm run dev`) |

## Stack

Go 1.27 · `rabbitmq/amqp091-go` · `jackc/pgx` (via `database/sql`) · `net/http` · RabbitMQ 3.13 · PostgreSQL 16 · Docker Compose

Interface web (`web/`): Next.js 16 · React 19 · `motion` · Tailwind CSS v4
