# projeto-trocas

Plataforma de permutas de itens, composta por microsserviços que se comunicam de forma assíncrona via Apache Kafka, com um BFF como único ponto de entrada HTTP.

## Requisitos

- [Docker](https://docs.docker.com/get-docker/) e [Docker Compose](https://docs.docker.com/compose/) (Docker Desktop já inclui ambos).
- Nenhuma outra dependência local é necessária — Python, PostgreSQL e Kafka rodam inteiramente dentro dos containers.
- Portas livres na máquina host: `8000`–`8004` (serviços), `8080` (Kafka UI), `9094` (Kafka), `5432` é usado apenas *dentro* da rede Docker (os bancos não expõem porta ao host).

## Instalação

```bash
git clone <repositório>
cd projeto-trocas
cp .env.example .env
```

O arquivo `.env` (ignorado pelo Git) concentra toda a configuração do sistema — portas, credenciais de banco, segredo JWT, parâmetros de resiliência etc. `.env.example` traz valores padrão prontos para desenvolvimento local; nenhuma edição é necessária para simplesmente subir o projeto, mas `JWT_SECRET_KEY` deve ser trocado antes de qualquer uso além de desenvolvimento/estudo.

## Execução

Com o Docker em execução, um único comando sobe toda a stack:

```bash
docker compose up --build
```

Isso inicia, na ordem correta (via `depends_on` + `healthcheck`): o broker Kafka e os quatro bancos PostgreSQL primeiro, seguidos pelos cinco microsserviços e pelo Kafka UI. Para rodar em segundo plano, adicione `-d`; para encerrar, `docker compose down` (ou `docker compose down -v` para também apagar os volumes/dados dos bancos).

Endpoints de verificação (`/health`) de cada serviço:

| Serviço | URL |
|---|---|
| BFF | http://localhost:8000/health |
| Users | http://localhost:8001/health |
| Ads | http://localhost:8002/health |
| Trades | http://localhost:8003/health |
| Notifications | http://localhost:8004/health |

Todas as requisições da aplicação (cadastro, login, anúncios, trocas, notificações) são feitas contra o **BFF**, na porta `8000` — os demais serviços não são chamados diretamente pelo cliente.

### Visualizando o Kafka no navegador (Kafka UI)

Com a stack no ar, acesse **http://localhost:8080**:

1. Clique no cluster **local**.
2. Vá em **Topics** e escolha um tópico (ex.: `users.registered`, `trades.accepted`, `dlq`).
3. Aba **Messages** → dá para ver o payload de cada evento publicado e consumido pelos serviços, incluindo mensagens enviadas à Dead Letter Queue.

## Arquitetura

```
Frontend (fora do escopo deste repositório)
        │
        ▼
      BFF  ──── único ponto de entrada HTTP
        │
        ▼
   Apache Kafka  ──── barramento de comandos/eventos entre todos os serviços
    │     │     │     │
    ▼     ▼     ▼     ▼
 Users   Ads  Trades Notifications
    │     │     │     │
    ▼     ▼     ▼     ▼
users_db ads_db trades_db notifications_db   (um Postgres por serviço)
```

- **BFF** (`services/bff`) — único ponto de entrada HTTP da aplicação. Não contém regra de negócio: recebe a requisição, valida o JWT quando aplicável, publica um comando no Kafka, aguarda a resposta (com timeout configurável) e traduz o evento recebido na resposta HTTP correspondente.
- **Users** (`services/users`) — cadastro, login (hash de senha com bcrypt, JWT HS256) e gerenciamento do perfil do usuário autenticado.
- **Ads** (`services/ads`) — CRUD de anúncios, busca de anúncios disponíveis e o comando interno `ads.get_by_id`/`ads.mark_unavailable` usado por outros serviços.
- **Trades** (`services/trades`) — solicitação, aceite, recusa e cancelamento de trocas entre anúncios; consulta o Ads via Kafka para validar posse/existência de anúncios, pois não tem acesso ao banco do Ads.
- **Notifications** (`services/notifications`) — consome eventos dos demais serviços e registra notificações; nenhum outro serviço depende dele.
- **Apache Kafka** — broker único de comunicação entre BFF e microsserviços, e entre microsserviços entre si (Trades ↔ Ads). Toda comunicação inter-serviços passa por aqui — não há chamadas HTTP diretas entre microsserviços.
- **PostgreSQL** — um banco isolado por microsserviço (`users_db`, `ads_db`, `trades_db`, `notifications_db`); nenhum serviço acessa o banco de outro.

Cada microsserviço é responsável por toda a regra de negócio do seu domínio; o BFF nunca decide nada sozinho — apenas encaminha e traduz.

## Estrutura do projeto

```
projeto-trocas/
├── services/
│   ├── bff/              # ponto de entrada HTTP
│   ├── users/             # cadastro, login, perfil
│   ├── ads/                # anúncios e busca
│   ├── trades/             # solicitações e decisões de troca
│   └── notifications/      # registro de notificações
├── docker-compose.yml
├── .env.example             # copie para .env antes de rodar
└── README.md
```

Cada microsserviço segue a mesma organização mínima:

```
<servico>/
├── app/
│   ├── main.py              # FastAPI app + lifespan (health check, start/stop do Kafka)
│   ├── config.py             # Settings (pydantic-settings), carregado do .env
│   ├── kafka_producer.py     # publica eventos/comandos
│   ├── kafka_consumer.py      # consome comandos/eventos (com retry + DLQ + idempotência)
│   ├── database.py           # (serviços com banco) engine + sessão SQLAlchemy
│   ├── models.py             # (serviços com banco) modelo ORM
│   ├── repository.py         # (serviços com banco) acesso a dados
│   ├── schemas.py             # modelos Pydantic dos comandos/eventos Kafka
│   └── commands.py            # regra de negócio: consome comando, produz evento
├── Dockerfile
└── requirements.txt
```

O BFF segue uma variação do mesmo padrão (`kafka_client.py` no lugar de `kafka_producer`/`kafka_consumer`, já que ele faz *request/reply* em vez de processar comandos), além de `security.py` (validação do JWT) e testes em `tests/`.

## Endpoints da API (via BFF, porta 8000)

| Método | Rota | Auth | Descrição |
|---|---|---|---|
| POST | `/register` | — | Cadastro de usuário |
| POST | `/login` | — | Autenticação, retorna JWT |
| GET | `/me` | JWT | Consultar o próprio perfil |
| PUT | `/me` | JWT | Atualizar nome/email |
| POST | `/ads` | JWT | Criar anúncio |
| GET | `/ads` | JWT | Listar meus anúncios |
| PUT | `/ads/{id}` | JWT | Atualizar anúncio (só o dono) |
| DELETE | `/ads/{id}` | JWT | Remover anúncio (só o dono) |
| GET | `/ads/search?q=` | JWT | Buscar anúncios disponíveis (exclui os próprios) |
| POST | `/trades` | JWT | Solicitar troca entre dois anúncios |
| POST | `/trades/{id}/accept` | JWT | Aceitar solicitação (só o dono do anúncio solicitado) |
| POST | `/trades/{id}/reject` | JWT | Recusar solicitação (só o dono do anúncio solicitado) |
| POST | `/trades/{id}/cancel` | JWT | Cancelar solicitação (só quem a criou) |
| GET | `/notifications` | JWT | Consultar minhas notificações |

Detalhes de request/response e os códigos de erro de cada endpoint estão descritos na seção de cada feature, mais abaixo.

## Fluxo integrado de ponta a ponta

1. **Cadastro** — `POST /register` cria o usuário no serviço Users.
2. **Login** — `POST /login` retorna um JWT.
3. **Criação de anúncios** — cada usuário cria seus anúncios via `POST /ads`.
4. **Busca** — um usuário busca anúncios de outros via `GET /ads/search`.
5. **Solicitação de troca** — `POST /trades`, informando o anúncio próprio e o anúncio alvo.
6. **Aceite ou recusa** — o dono do anúncio alvo decide via `POST /trades/{id}/accept` ou `/reject`; ao aceitar, ambos os anúncios ficam indisponíveis para novas trocas.
7. **Cancelamento** — enquanto pendente, o solicitante pode cancelar via `POST /trades/{id}/cancel`.
8. **Notificação** — a cada um desses eventos, o serviço Notifications registra uma notificação para o usuário correto, consultável via `GET /notifications`.

Esse fluxo foi validado de ponta a ponta contra a stack rodando em Docker Compose (registro de dois usuários, CRUD e busca de anúncios, solicitação/aceite/recusa/cancelamento de trocas, e consulta das notificações geradas em cada etapa).

## Resiliência

A comunicação via Kafka conta com mecanismos básicos de tolerância a falhas, transparentes para as funcionalidades de negócio:

- **Timeout** — o BFF limita a espera por respostas dos microsserviços (`BFF_KAFKA_REPLY_TIMEOUT_SECONDS`, `TRADES_KAFKA_REPLY_TIMEOUT_SECONDS`), retornando `504` quando o tempo é excedido.
- **Retry** — os consumidores Kafka de Users, Ads, Trades e Notifications tentam novamente o processamento de uma mensagem até `KAFKA_RETRY_ATTEMPTS` vezes, aguardando `KAFKA_RETRY_DELAY_SECONDS` entre tentativas.
- **Dead Letter Queue** — esgotadas as tentativas, a mensagem original (tópico, payload e motivo) é publicada no tópico `KAFKA_DLQ_TOPIC` (`dlq`), visível pelo Kafka UI.
- **Idempotência** — cada consumidor mantém em memória as mensagens já processadas na sessão (identificadas por tópico + partição + offset, metadados do próprio Kafka), ignorando reentregas.
- **Logs** — tentativas falhas, esgotamento de tentativas e envios à DLQ são registrados via `logging`.

## Histórico de features

<details>
<summary>Feature 000 — Bootstrap</summary>

Prepara apenas a infraestrutura: estrutura do monorepo, Docker Compose, Kafka, bancos PostgreSQL e servidores FastAPI com `/health`. Não implementa regras de negócio, WebSocket, ORM/persistência, autenticação ou comunicação entre microsserviços.

</details>

<details>
<summary>Feature 001 — Authentication</summary>

O BFF expõe cadastro e login, encaminhando os comandos ao serviço Users via Kafka:

- `POST /register` — `{"name", "email", "password"}` → `201` com `{"id", "name", "email"}`, ou `409` se o email já estiver cadastrado.
- `POST /login` — `{"email", "password"}` → `200` com `{"access_token", "token_type"}`, ou `401` se as credenciais forem inválidas.

O serviço Users é o único responsável por cadastro, login, hash de senha (bcrypt) e geração/validação de JWT (HS256). A senha nunca é armazenada em texto puro.

</details>

<details>
<summary>Feature 002 — Users</summary>

O BFF expõe a consulta e atualização do perfil do usuário autenticado, encaminhando os comandos ao serviço Users via Kafka:

- `GET /me` — requer `Authorization: Bearer <token>` → `200` com `{"id", "name", "email"}`.
- `PUT /me` — requer `Authorization: Bearer <token>` e `{"name", "email"}` → `200` com o perfil atualizado, ou `409` se o email já estiver em uso por outro usuário.

O BFF valida o JWT para identificar o usuário autenticado antes de encaminhar o comando; toda regra de negócio do perfil (unicidade de email, persistência) permanece no serviço Users.

</details>

<details>
<summary>Feature 003 — Ads</summary>

O BFF expõe o gerenciamento de anúncios do usuário autenticado, encaminhando os comandos ao serviço Ads via Kafka:

- `POST /ads` — requer `Authorization: Bearer <token>` e `{"title", "description"}` → `201` com `{"id", "title", "description"}`.
- `GET /ads` — requer `Authorization: Bearer <token>` → `200` com a lista dos anúncios do usuário.
- `PUT /ads/{id}` — requer `Authorization: Bearer <token>` e `{"title", "description"}` → `200` com o anúncio atualizado, `404` se o anúncio não existir, ou `403` se pertencer a outro usuário.
- `DELETE /ads/{id}` — requer `Authorization: Bearer <token>` → `204`, `404` se o anúncio não existir, ou `403` se pertencer a outro usuário.

O BFF identifica o usuário a partir do JWT (mesmo mecanismo da Feature 002) e encaminha o `owner_id` ao serviço Ads; toda regra de negócio (posse do anúncio, persistência) permanece no serviço Ads, que possui banco próprio (`ads_db`).

</details>

<details>
<summary>Feature 004 — Ad Search</summary>

O BFF expõe a consulta de anúncios disponíveis (de outros usuários), encaminhando os comandos ao serviço Ads via Kafka:

- `GET /ads/search` — requer `Authorization: Bearer <token>` → `200` com todos os anúncios disponíveis, exceto os do próprio usuário.
- `GET /ads/search?q=notebook` — mesmo endpoint, filtrando por título de forma parcial e case insensitive.

Reutiliza a tabela `ads` da Feature 003 (nenhuma tabela nova); a exclusão dos anúncios do próprio usuário e o filtro por título são resolvidos no serviço Ads.

</details>

<details>
<summary>Feature 005 — Trade Request</summary>

O BFF expõe a criação de solicitações de troca entre anúncios, encaminhando o comando ao serviço Trades via Kafka:

- `POST /trades` — requer `Authorization: Bearer <token>` e `{"requester_ad_id", "target_ad_id"}` → `201` com `{"id", "status": "PENDING"}`, ou falha (`404` se algum anúncio não existir, `400` se o anúncio de destino pertencer ao próprio solicitante).

O serviço Trades possui banco próprio (`trades_db`) e é responsável por toda a regra de negócio da solicitação. Como cada microsserviço tem seu próprio banco, o Trades não acessa a tabela `ads` diretamente: ele consulta o serviço Ads via Kafka (comando interno `ads.get_by_id`, respondido com `ads.found`/`ads.not_found`) para validar que os anúncios existem e que o anúncio de destino não pertence ao solicitante. Esta feature apenas cria a solicitação com status `PENDING`.

</details>

<details>
<summary>Feature 006 — Trade Decision</summary>

O BFF expõe o aceite e a recusa de uma solicitação de troca, encaminhando os comandos ao serviço Trades via Kafka:

- `POST /trades/{id}/accept` — requer `Authorization: Bearer <token>` → `200` com `{"status": "ACCEPTED"}`, ou falha (`404` se a troca ou o anúncio alvo não existir, `403` se o usuário não for o proprietário do anúncio solicitado, `409` se a troca não estiver mais `PENDING` ou se algum dos anúncios já participar de outra troca aceita).
- `POST /trades/{id}/reject` — mesma autenticação/autorização → `200` com `{"status": "REJECTED"}`, com as mesmas falhas possíveis (exceto o conflito de anúncio já negociado, que só se aplica ao aceite).

Reutiliza a tabela `trades` da Feature 005 (apenas o campo `status` é atualizado). Como somente o proprietário do anúncio solicitado (`target_ad_id`) pode decidir, o Trades consulta o serviço Ads via Kafka (`ads.get_by_id`) para confirmar o dono do anúncio antes de autorizar a decisão. Ao aceitar, o Trades garante — checando as próprias solicitações já `ACCEPTED` — que nenhum dos dois anúncios já participa de outra troca aceita, atualiza o status para `ACCEPTED` e avisa o serviço Ads (comando interno, best-effort, `ads.mark_unavailable`) para marcar ambos os anúncios como indisponíveis; a partir daí eles deixam de aparecer em `GET /ads/search`.

</details>

<details>
<summary>Feature 007 — Trade Cancel</summary>

O BFF expõe o cancelamento de uma solicitação de troca, encaminhando o comando ao serviço Trades via Kafka:

- `POST /trades/{id}/cancel` — requer `Authorization: Bearer <token>` → `200` com `{"status": "CANCELLED"}`, ou falha (`404` se a troca não existir, `403` se o usuário não for quem criou a solicitação, `409` se ela não estiver mais `PENDING`).

Reutiliza a tabela `trades` (apenas o campo `status` é atualizado) e o `requester_id` já armazenado na solicitação — diferente do aceite/recusa, o cancelamento não precisa consultar o serviço Ads, pois quem pode cancelar é sempre o próprio solicitante. O cancelamento só é permitido enquanto a troca estiver `PENDING` e não afeta anúncios já envolvidos em trocas aceitas.

</details>

<details>
<summary>Feature 008 — Notifications</summary>

Novo microsserviço **Notifications**, com banco próprio (`notifications_db`), que apenas consome eventos do Kafka e registra notificações — nenhum outro serviço depende dele.

- `GET /notifications` — requer `Authorization: Bearer <token>` → `200` com a lista das notificações do usuário autenticado (mais recentes primeiro): `[{"id", "type", "message", "created_at"}]`.

Eventos consumidos e a notificação gerada:

- `users.registered` → notifica o usuário cadastrado (`USER_REGISTERED`).
- `trades.requested` → notifica o dono do anúncio solicitado (`TRADE_REQUEST`).
- `trades.accepted` / `trades.rejected` → notifica quem fez a solicitação (`TRADE_ACCEPTED` / `TRADE_REJECTED`).
- `trades.cancelled` → notifica o dono do anúncio solicitado (`TRADE_CANCELLED`).

Como os eventos `trades.requested/accepted/rejected/cancelled` originais (Features 005–007) carregavam apenas `trade_id` e `status`, o serviço Trades passou a incluir também a identidade do usuário a ser notificado (`target_owner_id` ou `requester_id`, conforme o caso) — um campo adicional no payload, sem alterar os campos já existentes nem o comportamento dos consumidores atuais (BFF). O Notifications não expõe nem consome nenhum outro comando além da consulta; envio de e-mail/SMS/push, WebSocket e marcação de notificações como lidas ficam fora do escopo.

</details>

<details>
<summary>Feature 009 — Resilience</summary>

Mecanismos básicos de tolerância a falhas na comunicação via Kafka, transparentes para as funcionalidades já implementadas — nenhum contrato HTTP ou Kafka foi alterado. Ver a seção [Resiliência](#resiliência) acima para os detalhes; o timeout do BFF já existia desde as Features 000/001/005 e não precisou de alterações.

</details>

<details>
<summary>Feature 010 — Deployment</summary>

Integração final do sistema: revisão de todos os Dockerfiles e do `docker-compose.yml`, criação do `.env.example` (necessário para que `docker compose up` funcione a partir de um clone limpo do repositório, já que `.env` é ignorado pelo Git), e validação do fluxo completo ponta a ponta contra a stack real — cadastro, login, CRUD e busca de anúncios, solicitação/aceite/recusa/cancelamento de trocas, notificações e os mecanismos de resiliência (timeout, retry e DLQ, testados simulando a queda temporária do banco do serviço Ads). Nenhuma regra de negócio, contrato HTTP ou comando/evento Kafka foi alterado.

</details>
