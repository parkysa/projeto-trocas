# Feature 002 - Padrão de Camada de Banco de Dados

## Objetivo

Replicar nos serviços `ads` e `trades` a estrutura de banco de dados já funcionando em `users`. O padrão é sempre o mesmo: cinco arquivos com responsabilidades bem separadas, integrados ao FastAPI via `lifespan` e ao Docker Compose via `depends_on`.

Nenhum arquivo de `users` é alterado. O `bff` não é tocado.

---

## Estrutura de Arquivos

```text
services/
├── users/app/
│   ├── settings.py      ← já existe
│   ├── database.py      ← já existe
│   ├── models.py        ← atualizado (nickname, phone_number)
│   ├── schemas.py       ← já existe
│   ├── repository.py    ← já existe
│   └── main.py          ← já existe (com lifespan)
├── ads/app/
│   ├── settings.py      ← novo
│   ├── database.py      ← novo
│   ├── models.py        ← novo
│   ├── schemas.py       ← novo
│   ├── repository.py    ← novo
│   └── main.py          ← atualizado (adicionar lifespan)
└── trades/app/
    ├── settings.py      ← novo
    ├── database.py      ← novo
    ├── models.py        ← novo
    ├── schemas.py       ← novo
    ├── repository.py    ← novo
    └── main.py          ← atualizado (adicionar lifespan)
```

---

## settings.py

Carrega as credenciais do banco via `pydantic-settings`. Cada serviço usa um prefixo próprio nas variáveis de ambiente e expõe a propriedade `database_url` no formato `postgresql+asyncpg://`. A função `get_settings` usa `lru_cache` para ler as variáveis apenas uma vez por processo.

- `ads`: prefixo `ADS_`, host padrão `ads-db`
- `trades`: prefixo `TRADES_`, host padrão `trades-db`

As variáveis `*_DB_NAME`, `*_DB_USER` e `*_DB_PASSWORD` já estão definidas no `.env`.

---

## database.py

Cria o engine assíncrono via `create_async_engine` e o session maker via `async_sessionmaker` com `expire_on_commit=False`. Define a classe `Base(DeclarativeBase)` usada pelos models do serviço. Expõe `get_db` como dependency do FastAPI — abre uma `AsyncSession`, cede via `yield` e fecha no bloco `finally`.

O código é idêntico nos dois serviços. A única diferença é o módulo `settings` importado, que é local a cada serviço.

---

## models.py

Define o model ORM do serviço. Todo model herda de `Base` e inclui os mesmos campos de infraestrutura:

- `id` — UUID, chave primária, gerado por `uuid.uuid4`
- `created_at` — `DateTime(timezone=True)`, preenchido automaticamente no INSERT via `server_default`
- `updated_at` — `DateTime(timezone=True)`, atualizado automaticamente no UPDATE

Campos específicos por entidade:

**User** (`users`) — campos adicionados ao model já existente:
- `nickname` (String 50, nullable)
- `phone_number` (String 20, nullable)

**Ad** (`ads`):
- `title` (String 200)
- `description` (String 2000)
- `owner_id` (UUID, indexado)
- `address` (String 500) — endereço do local de publicação/retirada
- `publication_date` (Date) — data de publicação do anúncio
- `accept_terms` (String 500) — condições exigidas pelo autor para aceitar a permuta
- `item_condition` (String 20) — estado do item, ex: `"new"` ou `"used"`

**Trade** (`trades`):
- `ad_id` (UUID, indexado)
- `proposer_id` (UUID, indexado)
- `offered_ad_id` (UUID, indexado) — anúncio que o proponente está oferecendo em troca
- `status` (String 50, padrão `"pending"`)
- `purpose_date` (Date) — data em que a proposta foi feita
- `answer_date` (Date, nullable) — data em que a proposta foi aceita ou recusada

---

## schemas.py

Três schemas por serviço:

- **Create** — campos obrigatórios fornecidos pelo cliente na criação, sem valores padrão opcionais
- **Update** — todos os campos opcionais (`field | None = None`), para atualizações parciais via PATCH
- **Out** — campos de saída da API, com `model_config = {"from_attributes": True}` para conversão direta do model ORM; inclui sempre `id`, `created_at` e `updated_at`

---

## repository.py

Funções assíncronas de CRUD. A sessão é sempre recebida como parâmetro explícito. Em caso de falha no commit, o repository faz rollback e relança a exceção.

Funções por serviço:

- `get_<entity>(db, id)` — busca por UUID, retorna `None` se não encontrado
- `list_<entities>(db, skip=0, limit=100)` — listagem paginada
- `create_<entity>(db, schema)` — cria, faz commit e refresh
- `update_<entity>(db, entity, schema)` — aplica `model_dump(exclude_unset=True)`, faz commit e refresh
- `delete_<entity>(db, entity)` — deleta e faz commit

---

## main.py

O `main.py` de cada serviço registra o `lifespan` com `create_all` e expõe as rotas de CRUD da entidade, seguindo o padrão do serviço `users`.

**ads** — rotas:
- `POST /ads` — cria anúncio (HTTP 201)
- `GET /ads` — lista com paginação (`skip`, `limit`)
- `GET /ads/{ad_id}` — busca por UUID (HTTP 404 se não encontrado)
- `PATCH /ads/{ad_id}` — atualiza parcialmente (HTTP 404 se não encontrado)
- `DELETE /ads/{ad_id}` — exclui (HTTP 204, HTTP 404 se não encontrado)

**trades** — rotas:
- `POST /trades` — cria proposta (HTTP 201)
- `GET /trades` — lista com paginação (`skip`, `limit`)
- `GET /trades/{trade_id}` — busca por UUID (HTTP 404 se não encontrado)
- `PATCH /trades/{trade_id}` — atualiza parcialmente (HTTP 404 se não encontrado)
- `DELETE /trades/{trade_id}` — exclui (HTTP 204, HTTP 404 se não encontrado)

---

## requirements.txt

Os serviços `ads` e `trades` precisam das mesmas dependências de banco já usadas em `users`. Adicionar a `requirements.txt` de cada um:

- `sqlalchemy==2.0.36`
- `asyncpg==0.30.0`
- `pydantic-settings==2.7.1`

`alembic` não é necessário — migrations estão fora do escopo desta feature.

---

## Docker Compose

Os containers `ads-db` e `trades-db` já existem e já têm `healthcheck` com `pg_isready`. Apenas os blocos dos serviços `ads` e `trades` precisam ser ajustados.

**ads**

```yaml
  ads:
    build:
      context: .
      dockerfile: services/ads/Dockerfile
    environment:
      ADS_HOST: ${ADS_HOST}
      ADS_PORT: ${ADS_PORT}
      ADS_DB_NAME: ${ADS_DB_NAME}
      ADS_DB_USER: ${ADS_DB_USER}
      ADS_DB_PASSWORD: ${ADS_DB_PASSWORD}
      ADS_DB_HOST: ads-db
      ADS_DB_PORT: 5432
    ports:
      - "${ADS_PORT}:${ADS_PORT}"
    depends_on:
      ads-db:
        condition: service_healthy
    networks:
      - trocas-network
```

**trades**

```yaml
  trades:
    build:
      context: .
      dockerfile: services/trades/Dockerfile
    environment:
      TRADES_HOST: ${TRADES_HOST}
      TRADES_PORT: ${TRADES_PORT}
      TRADES_DB_NAME: ${TRADES_DB_NAME}
      TRADES_DB_USER: ${TRADES_DB_USER}
      TRADES_DB_PASSWORD: ${TRADES_DB_PASSWORD}
      TRADES_DB_HOST: trades-db
      TRADES_DB_PORT: 5432
    ports:
      - "${TRADES_PORT}:${TRADES_PORT}"
    depends_on:
      trades-db:
        condition: service_healthy
    networks:
      - trocas-network
```

---

## Fora do Escopo

- Migrations com Alembic ou qualquer outra ferramenta
- Autenticação, autorização ou regras de negócio complexas
- Rotas de API além das rotas CRUD de `ads` e `trades`
- Configuração de ambientes de produção ou staging
- Testes automatizados — cobertos em feature separada
- Alterações no serviço `users`
- Alterações no serviço `bff`
