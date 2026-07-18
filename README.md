# projeto-trocas

Plataforma de permutas de itens, composta por microsserviços.

## Arquitetura

- **BFF** (`services/bff`) — ponto de entrada da aplicação.
- **Users** (`services/users`) — domínio de usuários.
- **Ads** (`services/ads`) — domínio de anúncios.
- **Trades** (`services/trades`) — domínio das permutas.
- **Notifications** (`services/notifications`) — registro de notificações dos usuários.
- **Apache Kafka** — broker de comunicação entre os microsserviços.
- **PostgreSQL** — um banco independente por microsserviço (`users_db`, `ads_db`, `trades_db`, `notifications_db`).

## Estrutura

```
services/
  bff/
  users/
  ads/
  trades/
  notifications/
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
- Notifications: `GET http://localhost:8004/health`

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

## Feature 005 (Trade Request)

O BFF expõe a criação de solicitações de troca entre anúncios, encaminhando o comando ao serviço Trades via Kafka:

- `POST /trades` — requer `Authorization: Bearer <token>` e `{"requester_ad_id", "target_ad_id"}` → `201` com `{"id", "status": "PENDING"}`, ou falha (`404` se algum anúncio não existir, `400` se o anúncio de destino pertencer ao próprio solicitante).

O serviço Trades possui banco próprio (`trades_db`) e é responsável por toda a regra de negócio da solicitação. Como cada microsserviço tem seu próprio banco, o Trades não acessa a tabela `ads` diretamente: ele consulta o serviço Ads via Kafka (comando interno `ads.get_by_id`, respondido com `ads.found`/`ads.not_found`) para validar que os anúncios existem e que o anúncio de destino não pertence ao solicitante. Esta feature apenas cria a solicitação com status `PENDING` — aceite, recusa, cancelamento e notificações pertencem a features futuras.

## Feature 006 (Trade Decision)

O BFF expõe o aceite e a recusa de uma solicitação de troca, encaminhando os comandos ao serviço Trades via Kafka:

- `POST /trades/{id}/accept` — requer `Authorization: Bearer <token>` → `200` com `{"status": "ACCEPTED"}`, ou falha (`404` se a troca ou o anúncio alvo não existir, `403` se o usuário não for o proprietário do anúncio solicitado, `409` se a troca não estiver mais `PENDING` ou se algum dos anúncios já participar de outra troca aceita).
- `POST /trades/{id}/reject` — mesma autenticação/autorização → `200` com `{"status": "REJECTED"}`, com as mesmas falhas possíveis (exceto o conflito de anúncio já negociado, que só se aplica ao aceite).

Reutiliza a tabela `trades` da Feature 005 (apenas o campo `status` é atualizado). Como somente o proprietário do anúncio solicitado (`target_ad_id`) pode decidir, o Trades consulta o serviço Ads via Kafka (`ads.get_by_id`) para confirmar o dono do anúncio antes de autorizar a decisão. Ao aceitar, o Trades garante — checando as próprias solicitações já `ACCEPTED` — que nenhum dos dois anúncios já participa de outra troca aceita, atualiza o status para `ACCEPTED` e avisa o serviço Ads (comando interno, best-effort, `ads.mark_unavailable`) para marcar ambos os anúncios como indisponíveis; a partir daí eles deixam de aparecer em `GET /ads/search` (Feature 004). Cancelamento e notificações pertencem a features futuras.

## Feature 007 (Trade Cancel)

O BFF expõe o cancelamento de uma solicitação de troca, encaminhando o comando ao serviço Trades via Kafka:

- `POST /trades/{id}/cancel` — requer `Authorization: Bearer <token>` → `200` com `{"status": "CANCELLED"}`, ou falha (`404` se a troca não existir, `403` se o usuário não for quem criou a solicitação, `409` se ela não estiver mais `PENDING`).

Reutiliza a tabela `trades` (apenas o campo `status` é atualizado) e o `requester_id` já armazenado na solicitação — diferente do aceite/recusa, o cancelamento não precisa consultar o serviço Ads, pois quem pode cancelar é sempre o próprio solicitante. O cancelamento só é permitido enquanto a troca estiver `PENDING` e não afeta anúncios já envolvidos em trocas aceitas. Notificações pertencem a uma feature futura.

## Feature 008 (Notifications)

Novo microsserviço **Notifications**, com banco próprio (`notifications_db`), que apenas consome eventos do Kafka e registra notificações — nenhum outro serviço depende dele.

- `GET /notifications` — requer `Authorization: Bearer <token>` → `200` com a lista das notificações do usuário autenticado (mais recentes primeiro): `[{"id", "type", "message", "created_at"}]`.

Eventos consumidos e a notificação gerada:

- `users.registered` → notifica o usuário cadastrado (`USER_REGISTERED`).
- `trades.requested` → notifica o dono do anúncio solicitado (`TRADE_REQUEST`).
- `trades.accepted` / `trades.rejected` → notifica quem fez a solicitação (`TRADE_ACCEPTED` / `TRADE_REJECTED`).
- `trades.cancelled` → notifica o dono do anúncio solicitado (`TRADE_CANCELLED`).

Como os eventos `trades.requested/accepted/rejected/cancelled` originais (Features 005–007) carregavam apenas `trade_id` e `status`, o serviço Trades passou a incluir também a identidade do usuário a ser notificado (`target_owner_id` ou `requester_id`, conforme o caso) — um campo adicional no payload, sem alterar os campos já existentes nem o comportamento dos consumidores atuais (BFF). O Notifications não expõe nem consome nenhum outro comando além da consulta; envio de e-mail/SMS/push, WebSocket e marcação de notificações como lidas ficam fora do escopo.
