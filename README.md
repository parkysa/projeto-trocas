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

## Visualizando o Kafka no navegador (kafka-ui)

Com `docker compose up` rodando, acesse **http://localhost:8080**:

1. Clique no cluster **local**.
2. Vá em **Topics** e escolha um tópico (ex.: `users.registered`).
3. Aba **Messages** → dá para ver o payload de cada evento publicado pelo Users e consumido pelo BFF.

## Escopo da Feature 000 (Bootstrap)

Esta feature prepara apenas a infraestrutura: estrutura do monorepo, Docker Compose, Kafka, bancos PostgreSQL e servidores FastAPI com `/health`. Não implementa regras de negócio, WebSocket, ORM/persistência, autenticação ou comunicação entre microsserviços.

## Feature 001 (Authentication)

O BFF expõe cadastro e login, encaminhando os comandos ao serviço Users via Kafka:

- `POST /register` — `{"name", "email", "password"}` → `201` com `{"id", "name", "email"}`, ou `409` se o email já estiver cadastrado.
- `POST /login` — `{"email", "password"}` → `200` com `{"access_token", "token_type"}`, ou `401` se as credenciais forem inválidas.

O serviço Users é o único responsável por cadastro, login, hash de senha (bcrypt) e geração/validação de JWT (HS256). A senha nunca é armazenada em texto puro.

## Feature 002 (Users)

O BFF expõe a consulta e atualização do perfil do usuário autenticado, encaminhando os comandos ao serviço Users via Kafka:

- `GET /me` — requer `Authorization: Bearer <token>` → `200` com `{"id", "name", "email"}`.
- `PUT /me` — requer `Authorization: Bearer <token>` e `{"name", "email"}` → `200` com o perfil atualizado, ou `409` se o email já estiver em uso por outro usuário.

O BFF valida o JWT para identificar o usuário autenticado antes de encaminhar o comando; toda regra de negócio do perfil (unicidade de email, persistência) permanece no serviço Users.

## Feature 003 (Ads)

O BFF expõe o gerenciamento de anúncios do usuário autenticado, encaminhando os comandos ao serviço Ads via Kafka:

- `POST /ads` — requer `Authorization: Bearer <token>` e `{"title", "description"}` → `201` com `{"id", "title", "description"}`.
- `GET /ads` — requer `Authorization: Bearer <token>` → `200` com a lista dos anúncios do usuário.
- `PUT /ads/{id}` — requer `Authorization: Bearer <token>` e `{"title", "description"}` → `200` com o anúncio atualizado, `404` se o anúncio não existir, ou `403` se pertencer a outro usuário.
- `DELETE /ads/{id}` — requer `Authorization: Bearer <token>` → `204`, `404` se o anúncio não existir, ou `403` se pertencer a outro usuário.

O BFF identifica o usuário a partir do JWT (mesmo mecanismo da Feature 002) e encaminha o `owner_id` ao serviço Ads; toda regra de negócio (posse do anúncio, persistência) permanece no serviço Ads, que possui banco próprio (`ads_db`).

## Feature 004 (Ad Search)

O BFF expõe a consulta de anúncios disponíveis (de outros usuários), encaminhando os comandos ao serviço Ads via Kafka:

- `GET /ads/search` — requer `Authorization: Bearer <token>` → `200` com todos os anúncios disponíveis, exceto os do próprio usuário.
- `GET /ads/search?q=notebook` — mesmo endpoint, filtrando por título de forma parcial e case insensitive.

Reutiliza a tabela `ads` da Feature 003 (nenhuma tabela nova); a exclusão dos anúncios do próprio usuário e o filtro por título são resolvidos no serviço Ads.
