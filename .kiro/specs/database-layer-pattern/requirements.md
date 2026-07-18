# Feature 002 - Padrão de Camada de Banco de Dados

## Objetivo

Documentar e replicar o padrão de camada de banco de dados já implementado no serviço `users` para os serviços `ads` e `trades`. O padrão consiste em cinco arquivos com responsabilidades bem definidas (`settings.py`, `database.py`, `models.py`, `schemas.py`, `repository.py`) e na integração correta com o Docker Compose.

---

# Requisitos Funcionais

## RF-001 - Configuração de Conexão (settings.py)

Cada serviço deve ter um `settings.py` que carrega as credenciais do banco de dados a partir de variáveis de ambiente usando `pydantic-settings`. A classe de settings usa um prefixo específico do serviço (`ADS_`, `TRADES_`) e expõe a propriedade `database_url` no formato `postgresql+asyncpg://`. A função `get_settings` usa `lru_cache` para que as variáveis sejam lidas apenas uma vez por processo.

Para `ads`: prefixo `ADS_`, host padrão `ads-db`, variáveis `ADS_DB_NAME`, `ADS_DB_USER`, `ADS_DB_PASSWORD` já definidas no `.env`.

Para `trades`: prefixo `TRADES_`, host padrão `trades-db`, variáveis `TRADES_DB_NAME`, `TRADES_DB_USER`, `TRADES_DB_PASSWORD` já definidas no `.env`.

---

## RF-002 - Engine e Sessão Assíncrona (database.py)

Cada serviço deve ter um `database.py` que cria o `AsyncEngine` via `create_async_engine` e o `AsyncSessionLocal` via `async_sessionmaker` com `expire_on_commit=False`. O módulo define a classe `Base(DeclarativeBase)` usada por todos os models do serviço e expõe a função `get_db` como dependency do FastAPI — ela abre uma `AsyncSession`, cede via `yield` e fecha no bloco `finally`.

---

## RF-003 - Modelos ORM (models.py)

Cada serviço deve ter um `models.py` com ao menos um model que herda de `Base`. Todo model deve incluir:

- `id` — UUID, chave primária, gerado por `uuid.uuid4`
- `created_at` — `DateTime(timezone=True)`, preenchido automaticamente no INSERT via `server_default=func.now()`
- `updated_at` — `DateTime(timezone=True)`, atualizado automaticamente no UPDATE

Campos específicos por entidade:

**User** (`users`) — campos adicionados ao model já existente:
- `nickname` — apelido do usuário, opcional
- `phone_number` — número de telefone, opcional

**Ad** (`ads`):
- `title` — título do anúncio
- `description` — descrição do item
- `owner_id` — UUID do usuário dono do anúncio, indexado
- `address` — endereço do local de publicação/retirada
- `publication_date` — data de publicação do anúncio
- `accept_terms` — condições que o autor exige para aceitar a permuta (ex: "Qualquer coisa com valor parecido")
- `item_condition` — estado do item (novo ou usado)

**Trade** (`trades`):
- `ad_id` — UUID do anúncio alvo da proposta, indexado
- `proposer_id` — UUID do usuário que fez a proposta, indexado
- `offered_ad_id` — UUID do anúncio que o proponente está oferecendo em troca, indexado
- `status` — estado da proposta (padrão `"pending"`)
- `purpose_date` — data em que a proposta foi feita
- `answer_date` — data em que a proposta foi aceita ou recusada, nullable

---

## RF-004 - Schemas Pydantic (schemas.py)

Cada serviço deve ter um `schemas.py` com três schemas:

- **Create** — campos obrigatórios que o cliente fornece na criação, sem valores padrão opcionais.
- **Update** — todos os campos opcionais (`field | None = None`), permitindo atualizações parciais.
- **Out** — campos de saída da API, com `model_config = {"from_attributes": True}` para conversão direta do model ORM, incluindo `id`, `created_at` e `updated_at`.

---

## RF-005 - Operações de Banco de Dados (repository.py)

Cada serviço deve ter um `repository.py` com funções assíncronas de CRUD que recebem a sessão como parâmetro explícito:

- `get_<entity>(db, id)` — busca por UUID, retorna `None` se não encontrado
- `list_<entities>(db, skip=0, limit=100)` — listagem paginada
- `create_<entity>(db, schema)` — cria, faz commit e refresh, retorna o model
- `update_<entity>(db, entity, schema)` — aplica `model_dump(exclude_unset=True)`, faz commit e refresh
- `delete_<entity>(db, entity)` — deleta e faz commit

Em caso de falha no commit, o repository deve fazer rollback e relançar a exceção.

---

## RF-006 - Rotas CRUD (main.py)

O `main.py` de cada serviço deve expor as rotas de CRUD para sua entidade, seguindo o mesmo padrão já adotado em `users`.

Para `ads`:

- `POST /ads` — criar anúncio (HTTP 201)
- `GET /ads` — listar anúncios com paginação (`skip`, `limit`)
- `GET /ads/{ad_id}` — buscar anúncio por UUID (HTTP 404 se não encontrado)
- `PATCH /ads/{ad_id}` — atualizar parcialmente (HTTP 404 se não encontrado)
- `DELETE /ads/{ad_id}` — excluir (HTTP 204, HTTP 404 se não encontrado)

Para `trades`:

- `POST /trades` — criar proposta (HTTP 201)
- `GET /trades` — listar propostas com paginação (`skip`, `limit`)
- `GET /trades/{trade_id}` — buscar proposta por UUID (HTTP 404 se não encontrado)
- `PATCH /trades/{trade_id}` — atualizar parcialmente (HTTP 404 se não encontrado)
- `DELETE /trades/{trade_id}` — excluir (HTTP 204, HTTP 404 se não encontrado)

---

## RF-007 - Criação Automática de Tabelas (lifespan)

O `main.py` de cada serviço deve registrar um `lifespan` no FastAPI que executa `Base.metadata.create_all` via `engine.begin()` de forma assíncrona na inicialização, antes de a aplicação começar a aceitar requisições. Se o banco não estiver disponível, a inicialização deve falhar com erro claro.

---

## RF-008 - Integração com Docker Compose

Os serviços `ads` e `trades` no `docker-compose.yml` devem receber:

- Seção `depends_on` apontando para `ads-db` e `trades-db` respectivamente, com `condition: service_healthy`
- Variáveis de ambiente de banco de dados injetadas individualmente na seção `environment` (nome, usuário, senha, host e porta)

Os containers `ads-db` e `trades-db` já possuem `healthcheck` com `pg_isready` — apenas a configuração dos serviços precisa ser ajustada.

---

# Requisitos Não Funcionais

## RNF-001 - Stack tecnológica

Todos os serviços devem usar exatamente a mesma stack já adotada em `users`:

- **SQLAlchemy** com suporte assíncrono (`sqlalchemy[asyncio]`)
- **asyncpg** como driver PostgreSQL
- **pydantic-settings** para carregamento de variáveis de ambiente
- **FastAPI** com `lifespan` para ciclo de vida da aplicação

---

## RNF-002 - Consistência estrutural

A estrutura de arquivos de `ads` e `trades` deve espelhar a de `users`. Os nomes dos módulos (`settings.py`, `database.py`, `models.py`, `schemas.py`, `repository.py`) devem ser idênticos para facilitar navegação e manutenção.

---

## RNF-003 - Configuração sem segredos no código

Nenhuma credencial de banco de dados deve estar hardcoded. Todas as configurações sensíveis devem vir exclusivamente de variáveis de ambiente.

---

## Fora do Escopo

- Migrations com Alembic ou qualquer outra ferramenta — esta feature usa apenas `create_all` para ambiente de desenvolvimento
- Rotas de API além das rotas CRUD de `ads` e `trades` — autenticação, regras de negócio complexas e integrações ficam para features futuras
- Configuração de ambientes de produção ou staging
- Testes automatizados — cobertos em feature separada
- Alterações no serviço `bff`
