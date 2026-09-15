# message-queue-ecommerce

Pipeline assíncrono de pedidos de e-commerce usando **RabbitMQ**, em Go.

Trabalho Prático — Mensageria · Sistemas Distribuídos · FURB · Prof. Gabriel Castellani

## O que é

Simula o backend de uma loja virtual em que o checkout é desacoplado do
processamento: a API registra o pedido e publica um evento; pagamento, estoque e
notificação consomem esse evento de forma independente.

```
POST /pedidos ──► api ──► [ RabbitMQ ] ──► pagamento ──► estoque ──► notificacao
                                                      (fan-out para notificacao)
```

Demonstra roteamento por tópico, *fan-out*, *competing consumers*, retentativa
com espera, *dead letter queue* e idempotência.

## Documentação

| Documento | Conteúdo |
|---|---|
| [`docs/etapa1.md`](docs/etapa1.md) | Cenário e justificativa da mensageria |
| [`docs/etapa2.md`](docs/etapa2.md) | Arquitetura, fluxo, escalabilidade, confiabilidade, tolerância a falhas |
| [`docs/PRD.md`](docs/PRD.md) | Especificação de trabalho da equipe |
| [`docs/diagramas/`](docs/diagramas) | Diagramas (`.mmd` + `.png`) |

## Entregas

| Etapa | Conteúdo | Prazo | Pontos |
|---|---|---|---|
| 1 e 2 | Cenário + Arquitetura | 17/09/2026 | 2,5 |
| 3, 4 e 5 | Configuração + Casos de uso + Técnico | 24/09/2026 | 3,5 |
| Apresentação | 10 min | 24/09/2026 | 4,0 |

## Stack

Go 1.23 · RabbitMQ 3.13 · PostgreSQL 16 · Docker Compose

## Como rodar

> Em implementação. Ao final:

```bash
docker compose -f deploy/docker-compose.yml up
curl -X POST localhost:8080/pedidos -d @exemplo.json
```

Management UI: http://localhost:15672
