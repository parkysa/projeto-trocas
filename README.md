# projeto-trocas

Plataforma de permutas de itens, composta por microsserviços.

## Arquitetura

- **BFF** (`services/bff`) — ponto de entrada da aplicação.
- **Users** (`services/users`) — domínio de usuários.
- **Ads** (`services/ads`) — domínio de anúncios.
- **Trades** (`services/trades`) — domínio das permutas.
- **Apache Kafka** — broker disponível para comunicação futura entre serviços.
- **PostgreSQL** — um banco independente por microsserviço (`users_db`, `ads_db`, `trades_db`).

## Estrutura

```
services/
  bff/
  users/
  ads/
  trades/
docker-compose.yml
.env
```

Cada serviço segue a mesma organização mínima:

```
<servico>/
  app/
    main.py
  Dockerfile
  requirements.txt
```

## Executando

Todas as configurações vêm do arquivo `.env` na raiz do projeto.

```bash
docker compose up --build
```

Endpoints de verificação:

- BFF: `GET http://localhost:8000/health`
- Users: `GET http://localhost:8001/health`
- Ads: `GET http://localhost:8002/health`
- Trades: `GET http://localhost:8003/health`

## Escopo da Feature 000 (Bootstrap)

Esta feature prepara apenas a infraestrutura: estrutura do monorepo, Docker Compose, Kafka, bancos PostgreSQL e servidores FastAPI com `/health`. Não implementa regras de negócio, WebSocket, ORM/persistência, autenticação ou comunicação entre microsserviços.
