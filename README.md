# projeto-trocas

Plataforma de permutas de itens, composta por microsserviços que se comunicam de forma assíncrona via Apache Kafka. O cliente se comunica com o sistema através de um único canal **WebSocket**, servido pelo BFF.

## Requisitos

- [Docker](https://docs.docker.com/get-docker/) e [Docker Compose](https://docs.docker.com/compose/) (Docker Desktop já inclui ambos).
- Nenhuma outra dependência local é necessária — Python, PostgreSQL e Kafka rodam inteiramente dentro dos containers.
- Um cliente WebSocket para testar manualmente (ex.: `wscat`, a extensão WebSocket do Insomnia/Postman, ou um script Python com a biblioteca `websockets`) — não há mais endpoints HTTP de negócio para testar via navegador ou `curl`.
- Portas livres na máquina host: `8000`–`8004` (serviços), `8080` (Kafka UI), `9094` (Kafka). As portas dos bancos (`5432`) não são expostas ao host — inclusive as réplicas, acessíveis apenas via `docker compose exec`.

## Instalação

```bash
git clone <repositório>
cd projeto-trocas
cp .env.example .env
```

O arquivo `.env` (ignorado pelo Git) concentra toda a configuração do sistema — portas, credenciais de banco (inclusive de replicação), segredo JWT, parâmetros de resiliência etc. `.env.example` traz valores padrão prontos para desenvolvimento local; nenhuma edição é necessária para simplesmente subir o projeto, mas `JWT_SECRET_KEY` deve ser trocado antes de qualquer uso além de desenvolvimento/estudo.

## Execução

Com o Docker em execução, um único comando sobe toda a stack:

```bash
docker compose up --build
```

Isso inicia, na ordem correta (via `depends_on` + `healthcheck`): o broker Kafka e os 4 pares de banco PostgreSQL primário+réplica primeiro, seguidos pelos cinco microsserviços e pelo Kafka UI. Para rodar em segundo plano, adicione `-d`; para encerrar, `docker compose down` (ou `docker compose down -v` para também apagar os volumes/dados dos bancos — nesse caso as réplicas re-clonam automaticamente dos primários na próxima subida).

Endpoints de verificação (`/health`, HTTP simples) de cada serviço:

| Serviço | URL |
|---|---|
| BFF | http://localhost:8000/health |
| Users | http://localhost:8001/health |
| Ads | http://localhost:8002/health |
| Trades | http://localhost:8003/health |
| Notifications | http://localhost:8004/health |

Toda a comunicação de negócio (cadastro, login, anúncios, trocas, notificações) acontece via **WebSocket em `ws://localhost:8000/ws`** — os demais serviços não são chamados diretamente pelo cliente.

### Visualizando o Kafka no navegador (Kafka UI)

Com a stack no ar, acesse **http://localhost:8080**:

1. Clique no cluster **local**.
2. Vá em **Topics** e escolha um tópico (ex.: `users.usuario.cadastrado`, `trades.troca.aprovada`, `dlq`).
3. Aba **Messages** → dá para ver o payload de cada evento publicado e consumido pelos serviços, incluindo mensagens enviadas à Dead Letter Queue. O corpo de cada mensagem segue o envelope `{"tipo", "topico", "payload"}` descrito abaixo.

## Arquitetura

```
Frontend (fora do escopo deste repositório)
        │  WebSocket (ws://.../ws)
        ▼
      BFF  ──── único ponto de entrada do cliente
        │
        ▼
   Apache Kafka  ──── barramento de comandos/eventos entre todos os serviços
    │     │     │     │
    ▼     ▼     ▼     ▼
 Users   Ads  Trades Notifications
    │     │     │     │
    ▼     ▼     ▼     ▼
 primário  primário  primário  primário     (SQLAlchemy assíncrono + asyncpg)
    │        │         │         │
    ▼        ▼         ▼         ▼
 réplica   réplica   réplica   réplica      (streaming replication, somente leitura/backup)
```

- **BFF** (`services/bff`) — único ponto de entrada do cliente, via WebSocket. Não contém regra de negócio: recebe a mensagem, valida o JWT quando aplicável, publica um comando/consulta no Kafka, aguarda a resposta (com timeout configurável) e encaminha o evento recebido de volta ao cliente pelo mesmo socket.
- **Users** (`services/users`) — cadastro, login (hash de senha com bcrypt, JWT HS256) e gerenciamento do perfil do usuário autenticado.
- **Ads** (`services/ads`) — CRUD de anúncios, busca de anúncios disponíveis e o comando interno de consulta/marcação usado por outros serviços.
- **Trades** (`services/trades`) — solicitação, aceite, recusa e cancelamento de trocas entre anúncios; consulta o Ads via Kafka para validar posse/existência de anúncios, pois não tem acesso ao banco do Ads.
- **Notifications** (`services/notifications`) — consome eventos dos demais serviços e registra notificações; nenhum outro serviço depende dele.
- **Apache Kafka** — broker único de comunicação entre BFF e microsserviços, e entre microsserviços entre si (Trades ↔ Ads). Toda comunicação inter-serviços passa por aqui — não há chamadas HTTP diretas entre microsserviços.
- **PostgreSQL** — um banco isolado por microsserviço (`users_db`, `ads_db`, `trades_db`, `notifications_db`), cada um com um par primário + réplica em streaming replication (ver seção [Replicação de banco](#replicação-de-banco)); a aplicação só lê/escreve no primário.

Cada microsserviço é responsável por toda a regra de negócio do seu domínio; o BFF nunca decide nada sozinho — apenas encaminha e traduz.

## Estrutura do projeto

```
projeto-trocas/
├── services/
│   ├── bff/              # ponto de entrada WebSocket
│   ├── users/             # cadastro, login, perfil
│   ├── ads/                # anúncios e busca
│   ├── trades/             # solicitações e decisões de troca
│   └── notifications/      # registro de notificações
├── scripts/
│   └── postgres/           # scripts de setup de replicação (primário/réplica)
├── docker-compose.yml
├── .env.example             # copie para .env antes de rodar
└── README.md
```

Cada microsserviço com banco segue a mesma organização mínima:

```
<servico>/
├── app/
│   ├── main.py               # FastAPI app + lifespan (health check, start/stop do Kafka)
│   ├── config.py              # Settings (pydantic-settings), carregado do .env
│   ├── kafka_producer.py       # monta o envelope {"tipo","topico","payload"} + headers e publica
│   ├── kafka_consumer.py        # consome comandos/eventos (com retry + DLQ + idempotência)
│   ├── database.py              # engine/sessão assíncrona (SQLAlchemy + asyncpg)
│   ├── models.py                 # modelo ORM
│   ├── repository.py              # acesso a dados (métodos assíncronos)
│   ├── schemas.py                  # modelos Pydantic dos comandos/eventos Kafka
│   └── commands.py                 # regra de negócio: consome comando, produz evento
├── Dockerfile
└── requirements.txt
```

O BFF segue uma variação do mesmo padrão: `kafka_client.py` no lugar de `kafka_producer`/`kafka_consumer` (faz *request/reply* em vez de processar comandos), `security.py` (decodifica o JWT recebido na query string da conexão WebSocket) e um único `@app.websocket("/ws")` em `main.py` no lugar de rotas REST, além dos testes em `tests/` (usando `TestClient.websocket_connect`).

## Protocolo WebSocket (`ws://localhost:8000/ws`)

O cliente conecta uma única vez (opcionalmente informando `?token=<jwt>` na URL, para as ações autenticadas) e, pelo mesmo socket, envia comandos/consultas e recebe eventos de volta — sem abrir uma conexão nova a cada interação.

Toda mensagem, em ambas as direções, segue o mesmo envelope:

```json
{
  "tipo": "Comando" | "Evento" | "Consulta",
  "topico": "dominio.entidade.acao",
  "payload": { }
}
```

O cliente envia `tipo: "Comando"` (altera estado) ou `"Consulta"` (só leitura); o BFF sempre responde com `tipo: "Evento"`, no mesmo `topico` do evento de resultado (sucesso ou falha) publicado pelo microsserviço responsável. IDs de recurso que antes eram parte da URL (`/ads/{id}`, `/trades/{id}`) agora vão dentro do `payload`, com a chave `id`.

| Ação do cliente (`topico`) | `tipo` | `payload` de entrada | Requer token? |
|---|---|---|---|
| `users.usuario.cadastrar` | Comando | `{"name", "email", "password"}` | não |
| `users.usuario.autenticar` | Comando | `{"email", "password"}` | não |
| `users.perfil.consultar` | Consulta | `{}` | sim |
| `users.perfil.atualizar` | Comando | `{"name", "email"}` | sim |
| `ads.anuncio.criar` | Comando | `{"title", "description"}` | sim |
| `ads.anuncio.consultar_proprios` | Consulta | `{}` | sim |
| `ads.anuncio.atualizar` | Comando | `{"id", "title", "description"}` | sim |
| `ads.anuncio.remover` | Comando | `{"id"}` | sim |
| `ads.anuncio.consultar_disponiveis` | Consulta | `{}` | sim |
| `ads.anuncio.buscar` | Consulta | `{"q"}` | sim |
| `trades.troca.solicitar` | Comando | `{"requester_ad_id", "target_ad_id"}` | sim |
| `trades.troca.aceitar` | Comando | `{"id"}` | sim |
| `trades.troca.recusar` | Comando | `{"id"}` | sim |
| `trades.troca.cancelar` | Comando | `{"id"}` | sim |
| `notifications.notificacao.consultar` | Consulta | `{}` | sim |

Falhas (validação, autorização, regra de negócio) chegam como um evento cujo `topico` é o mesmo tópico de falha publicado pelo microsserviço (ex.: `ads.anuncio.operacao_falhou`, `trades.troca.decisao_falhou`), com `payload.reason` explicando o motivo. Chamar uma ação autenticada sem token válido responde com `topico` = `"<ação>_nao_autorizado"` e `payload.reason` igual a `missing_token` ou `invalid_token`.

Exemplo de sessão (cadastro seguido de consulta de perfil):

```json
→ {"tipo": "Comando", "topico": "users.usuario.cadastrar", "payload": {"name": "João", "email": "joao@email.com", "password": "12345678"}}
← {"tipo": "Evento", "topico": "users.usuario.cadastrado", "payload": {"id": "...", "name": "João", "email": "joao@email.com"}}

→ {"tipo": "Comando", "topico": "users.usuario.autenticar", "payload": {"email": "joao@email.com", "password": "12345678"}}
← {"tipo": "Evento", "topico": "users.usuario.autenticado", "payload": {"access_token": "...", "token_type": "bearer"}}
```

Basta então conectar novamente em `ws://localhost:8000/ws?token=<access_token>` para as ações autenticadas.

## Formato das mensagens Kafka

Internamente (BFF↔microsserviços e microsserviço↔microsserviço, ex.: Trades↔Ads), toda mensagem Kafka usa o mesmo envelope do protocolo do cliente — `{"tipo", "topico", "payload"}` no corpo — mais um conjunto de headers Kafka para rastreabilidade: `event_id` (identificador único da mensagem), `correlation_id` (correlaciona comando/consulta com sua resposta), `timestamp`, `producer` (serviço publicador), `version` e, em comandos/consultas, `reply_to`.

Os tópicos seguem a convenção `<serviço>.<entidade>.<ação>` (serviço em inglês, entidade e ação em português) — comandos no infinitivo, eventos no particípio. Exemplos: `ads.anuncio.criar` (comando) → `ads.anuncio.criado` (evento); `trades.troca.aceitar` (comando) → `trades.troca.aprovada` (evento de sucesso) ou `trades.troca.decisao_falhou` (evento de falha).

## Replicação de banco

Cada um dos 4 bancos de dados (`users_db`, `ads_db`, `trades_db`, `notifications_db`) roda como um par **primário + réplica** com streaming replication nativa do PostgreSQL, configurado automaticamente por `docker compose up` via os scripts em `scripts/postgres/`:

- `primary-init.sh` roda uma única vez, na inicialização do primário, criando o usuário de replicação e liberando `pg_hba.conf` para conexões de replicação.
- `standby-entrypoint.sh` roda no container da réplica: se o diretório de dados estiver vazio, clona o primário via `pg_basebackup -R` (com algumas tentativas, para tolerar o pequeno intervalo em que o primário reinicia após rodar os scripts de inicialização) e já configura a réplica para entrar em modo *standby* automaticamente.

A aplicação (SQLAlchemy) só lê e escreve no **primário** — a réplica existe exclusivamente como cópia de segurança, somente leitura, e não é consultada pelo código da aplicação. Isso foi validado manualmente: dados escritos via o WebSocket aparecem na réplica ao consultá-la diretamente (`docker compose exec users-db-replica psql ...`), uma tentativa de escrita direta na réplica é rejeitada (`cannot execute INSERT in a read-only transaction`), e a réplica retoma o streaming automaticamente após um restart, sem precisar reclonar.

## Fluxo integrado de ponta a ponta

1. **Cadastro** — `users.usuario.cadastrar` cria o usuário no serviço Users.
2. **Login** — `users.usuario.autenticar` retorna um JWT.
3. **Criação de anúncios** — cada usuário cria seus anúncios via `ads.anuncio.criar`.
4. **Busca** — um usuário busca anúncios de outros via `ads.anuncio.consultar_disponiveis`/`ads.anuncio.buscar`.
5. **Solicitação de troca** — `trades.troca.solicitar`, informando o anúncio próprio e o anúncio alvo.
6. **Aceite ou recusa** — o dono do anúncio alvo decide via `trades.troca.aceitar` ou `trades.troca.recusar`; ao aceitar, ambos os anúncios ficam indisponíveis para novas trocas.
7. **Cancelamento** — enquanto pendente, o solicitante pode cancelar via `trades.troca.cancelar`.
8. **Notificação** — a cada um desses eventos, o serviço Notifications registra uma notificação para o usuário correto, consultável via `notifications.notificacao.consultar`.

Esse fluxo foi validado de ponta a ponta contra a stack rodando em Docker Compose, via um cliente WebSocket real (registro de dois usuários, CRUD e busca de anúncios, solicitação/aceite de trocas com checagem de autorização, e consulta das notificações geradas em cada etapa).

## Resiliência

A comunicação via Kafka conta com mecanismos básicos de tolerância a falhas, transparentes para as funcionalidades de negócio:

- **Timeout** — o BFF limita a espera por respostas dos microsserviços (`BFF_KAFKA_REPLY_TIMEOUT_SECONDS`, `TRADES_KAFKA_REPLY_TIMEOUT_SECONDS`), respondendo com um evento de timeout quando o tempo é excedido.
- **Retry** — os consumidores Kafka de Users, Ads, Trades e Notifications tentam novamente o processamento de uma mensagem até `KAFKA_RETRY_ATTEMPTS` vezes, aguardando `KAFKA_RETRY_DELAY_SECONDS` entre tentativas.
- **Dead Letter Queue** — esgotadas as tentativas, a mensagem original (tópico, payload e motivo) é publicada no tópico `KAFKA_DLQ_TOPIC` (`dlq`), visível pelo Kafka UI.
- **Idempotência** — cada consumidor mantém em memória as mensagens já processadas na sessão (identificadas por tópico + partição + offset, metadados do próprio Kafka), ignorando reentregas.
- **Logs** — tentativas falhas, esgotamento de tentativas e envios à DLQ são registrados via `logging`.

## Alinhamento com a especificação acadêmica (PDF)

Após a Feature 010, o sistema foi comparado com o PDF de especificação acadêmica da disciplina e cinco pontos de divergência foram corrigidos:

1. **WebSocket** — o REST do BFF foi totalmente substituído por um único endpoint WebSocket (`/ws`), como descrito no PDF.
2. **Envelope de mensagens** — todo comando/evento/consulta (cliente↔BFF e entre microsserviços via Kafka) passou a seguir o envelope `{"tipo", "topico", "payload"}` + headers (`event_id`, `correlation_id`, `timestamp`, `producer`, `version`, `topic`, `reply_to`).
3. **Nomenclatura de tópicos** — todos os tópicos Kafka foram renomeados para o padrão `<serviço>.<entidade>.<ação>` em português (ex.: `ads.anuncio.criado`, `trades.troca.aprovada`), batendo com os exemplos do próprio PDF.
4. **ORM assíncrono** — Users, Ads, Trades e Notifications passaram de SQLAlchemy síncrono (rodando em threads) para SQLAlchemy assíncrono nativo com `asyncpg`.
5. **Replicação de banco** — cada banco ganhou uma réplica em streaming replication, como descrito na seção [Replicação de banco](#replicação-de-banco) acima.

Nenhuma regra de negócio foi alterada nesse processo — apenas o transporte cliente↔BFF, o formato/nome das mensagens Kafka, o driver de banco e a topologia de infraestrutura dos bancos. Dois pontos de divergência identificados na comparação **não** foram tratados neste alinhamento, por decisão explícita: a existência do microsserviço Notifications (não previsto no PDF original) e o mecanismo de DLQ (adição da Feature 009, também não previsto no PDF) — ambos permanecem como estão.

## Histórico de features

> As seções abaixo documentam o desenvolvimento incremental do sistema (Features 000–010), quando a comunicação cliente↔BFF ainda era HTTP/REST. Os exemplos de rota e código de status HTTP nelas descritos são um registro histórico da implementação em cada etapa; a interface atual do sistema é a descrita nas seções acima (WebSocket, envelope de mensagens, tópicos renomeados).

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

O serviço Trades possui banco próprio (`trades_db`) e é responsável por toda a regra de negócio da solicitação. Como cada microsserviço tem seu próprio banco, o Trades não acessa a tabela `ads` diretamente: ele consulta o serviço Ads via Kafka (comando interno de consulta por id) para validar que os anúncios existem e que o anúncio de destino não pertence ao solicitante. Esta feature apenas cria a solicitação com status `PENDING`.

</details>

<details>
<summary>Feature 006 — Trade Decision</summary>

O BFF expõe o aceite e a recusa de uma solicitação de troca, encaminhando os comandos ao serviço Trades via Kafka:

- `POST /trades/{id}/accept` — requer `Authorization: Bearer <token>` → `200` com `{"status": "ACCEPTED"}`, ou falha (`404` se a troca ou o anúncio alvo não existir, `403` se o usuário não for o proprietário do anúncio solicitado, `409` se a troca não estiver mais `PENDING` ou se algum dos anúncios já participar de outra troca aceita).
- `POST /trades/{id}/reject` — mesma autenticação/autorização → `200` com `{"status": "REJECTED"}`, com as mesmas falhas possíveis (exceto o conflito de anúncio já negociado, que só se aplica ao aceite).

Reutiliza a tabela `trades` da Feature 005 (apenas o campo `status` é atualizado). Como somente o proprietário do anúncio solicitado (`target_ad_id`) pode decidir, o Trades consulta o serviço Ads via Kafka para confirmar o dono do anúncio antes de autorizar a decisão. Ao aceitar, o Trades garante — checando as próprias solicitações já `ACCEPTED` — que nenhum dos dois anúncios já participa de outra troca aceita, atualiza o status para `ACCEPTED` e avisa o serviço Ads (comando interno, best-effort) para marcar ambos os anúncios como indisponíveis; a partir daí eles deixam de aparecer na busca (Feature 004).

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

Como os eventos de troca originais (Features 005–007) carregavam apenas o identificador da troca e o status, o serviço Trades passou a incluir também a identidade do usuário a ser notificado — um campo adicional no payload, sem alterar os campos já existentes nem o comportamento dos consumidores. O Notifications não expõe nem consome nenhum outro comando além da consulta; envio de e-mail/SMS/push, WebSocket e marcação de notificações como lidas ficam fora do escopo.

</details>

<details>
<summary>Feature 009 — Resilience</summary>

Mecanismos básicos de tolerância a falhas na comunicação via Kafka, transparentes para as funcionalidades já implementadas — nenhum contrato HTTP ou Kafka foi alterado nesta feature. Ver a seção [Resiliência](#resiliência) acima para os detalhes; o timeout do BFF já existia desde as Features 000/001/005 e não precisou de alterações.

</details>

<details>
<summary>Feature 010 — Deployment</summary>

Integração final do sistema: revisão de todos os Dockerfiles e do `docker-compose.yml`, criação do `.env.example` (necessário para que `docker compose up` funcione a partir de um clone limpo do repositório, já que `.env` é ignorado pelo Git), e validação do fluxo completo ponta a ponta contra a stack real — cadastro, login, CRUD e busca de anúncios, solicitação/aceite/recusa/cancelamento de trocas, notificações e os mecanismos de resiliência (timeout, retry e DLQ, testados simulando a queda temporária do banco do serviço Ads). Nenhuma regra de negócio, contrato HTTP ou comando/evento Kafka foi alterado nesta feature — essas mudanças vieram depois, no alinhamento com o PDF acadêmico descrito acima.

</details>
