# Feature 002 - Padrão de Camada de Banco de Dados - Tasks

> **Ordem de implementação:** os serviços devem ser implementados **sequencialmente**. Primeiro atualizar `users`, depois implementar `ads` completo, e por último `trades`.

---

## Serviço users

- [ ] Adicionar os campos `nickname` (String 50, nullable) e `phone_number` (String 20, nullable) ao model `User` em `models.py`
- [ ] Atualizar `schemas.py` para incluir `nickname` e `phone_number` nos schemas `UserUpdate` e `UserOut`

### Validação

- [ ] Verificar se o container `users` sobe sem erro após o build
- [ ] Verificar se a tabela `users` é recriada com os novos campos no banco `users-db`

---

## Serviço ads

- [ ] Adicionar `sqlalchemy==2.0.36`, `asyncpg==0.30.0` e `pydantic-settings==2.7.1` ao `requirements.txt`
- [ ] Criar `settings.py` com `pydantic-settings`, prefixo `ADS_`, propriedade `database_url` e `get_settings` com `lru_cache`
- [ ] Criar `database.py` com `AsyncEngine`, `AsyncSessionLocal`, `Base` e função `get_db`
- [ ] Criar `models.py` com o model `Ad` (campos: `id`, `title`, `description`, `owner_id`, `address`, `publication_date`, `accept_terms`, `item_condition`, `created_at`, `updated_at`)
- [ ] Criar `schemas.py` com `AdCreate`, `AdUpdate` e `AdOut`
- [ ] Criar `repository.py` com as funções `get_ad`, `list_ads`, `create_ad`, `update_ad` e `delete_ad`
- [ ] Atualizar `main.py` para registrar `lifespan`, `create_all` e as rotas CRUD (`POST`, `GET`, `GET /{id}`, `PATCH /{id}`, `DELETE /{id}`)

---

## Serviço trades

- [ ] Adicionar `sqlalchemy==2.0.36`, `asyncpg==0.30.0` e `pydantic-settings==2.7.1` ao `requirements.txt`
- [ ] Criar `settings.py` com `pydantic-settings`, prefixo `TRADES_`, propriedade `database_url` e `get_settings` com `lru_cache`
- [ ] Criar `database.py` com `AsyncEngine`, `AsyncSessionLocal`, `Base` e função `get_db`
- [ ] Criar `models.py` com o model `Trade` (campos: `id`, `ad_id`, `proposer_id`, `status`, `purpose_date`, `answer_date`, `created_at`, `updated_at`)
- [ ] Criar `schemas.py` com `TradeCreate`, `TradeUpdate` e `TradeOut`
- [ ] Criar `repository.py` com as funções `get_trade`, `list_trades`, `create_trade`, `update_trade` e `delete_trade`
- [ ] Atualizar `main.py` para registrar `lifespan`, `create_all` e as rotas CRUD (`POST`, `GET`, `GET /{id}`, `PATCH /{id}`, `DELETE /{id}`)
- [ ] Adicionar variáveis de ambiente de banco (`TRADES_DB_NAME`, `TRADES_DB_USER`, `TRADES_DB_PASSWORD`, `TRADES_DB_HOST`, `TRADES_DB_PORT`) no serviço `trades` do Docker Compose
- [ ] Adicionar `depends_on` com `condition: service_healthy` apontando para `trades-db` no serviço `trades` do Docker Compose

### Validação

- [ ] Verificar se o container `trades` sobe sem erro após o build
- [ ] Verificar se a tabela `trades` é criada automaticamente no banco `trades-db`
- [ ] Verificar se `GET /health` continua respondendo no serviço `trades` (porta 8003)
- [ ] Verificar se as rotas CRUD respondem corretamente em `http://localhost:8003/docs`

---

## Validação Final

- [ ] Verificar se `docker compose up --build` sobe todos os containers juntos sem erro

---

## Fora do Escopo

Esta feature **não** deve implementar:

- migrations com Alembic ou qualquer outra ferramenta;
- autenticação, autorização ou regras de negócio complexas;
- rotas de API além das rotas CRUD de `ads` e `trades`;
- testes automatizados;
- alterações no serviço `users`;
- alterações no serviço `bff`.
