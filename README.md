# ⚔️ Lux Idle — Bot Autônomo & Multi-Contas (Go Native Engine)

Bot autônomo completo de altíssima performance desenvolvido em **Go (Golang)** para o jogo [Lux Idle](https://luxidle.com.br). Projetado com arquitetura nativa em Goroutines, baixo consumo de memória (< 10 MB de RAM), isolamento por IP na VPS e proteção contra detecções via **Circuit Breaker Anti-Cheat**.

---

## 🌟 Funcionalidades do Bot (Tudo que o Jogo Oferece)

1. 🏹 **Auto-Hunt Inteligente:**
   - Seleção dinâmica do melhor mapa de caça baseado no level do personagem (`catalog.hunts`).
   - Suporte a modo `online` ou `auto`.
   - Estratégias configuráveis de mira: Mais perto (`nearest`), Menos vida (`lowest`) ou Mais vida (`highest`).
   - Controle de distância (0 sqm corpo a corpo até 6 sqm ranged).
   - Sistema de lure configurável (0 a 10 monstros).
   - Disparo de tokens de ação canônicos (`MN(startHunt)`).

2. ⚔️ **Auto-Assistência & Barra Automática (`huntAssist`):**
   - Ativação contínua do auto-ataque e da barra de magias/curas/poções do jogo.

3. 🛡️ **Retirada de Emergência por HP:**
   - Monitoramento contínuo de HP/Mana. Se a vida cair abaixo da porcentagem de segurança configurada (`minHpPercent`), o bot interrompe a caçada (`stopHunt`) e retorna para a cidade para regenerar com total segurança.

4. 💰 **Auto-Venda & Limpeza de Inventário (`AutoSell`):**
   - Venda periódica das sacolas de loot (`venderSacolas`).
   - Venda da mochila de itens (`venderMochila`).
   - Limpeza e organização do boss pouch (`clearPouch`).

5. ✨ **Auto-Bênçãos (`AutoBless`):**
   - Compra automática de todas as bênçãos (`bless all`) no level 20+ ou após mortes.

6. 🎁 **Recompensas Diárias & Quests (`AutoDaily`):**
   - Resgate automático diário de recompensas do baú (`reward takeAll`).
   - Acompanhamento do progresso de tarefas diárias (`diario`).

7. 📜 **Tasks de Monstros (`AutoTasks`):**
   - Consulta e resgate automático de tarefas concluídas do bestiário (`tasksDeBicho`).

8. 👑 **Rotação Automática de Bosses (`AutoBoss`):**
   - Varredura de bosses disponíveis e fora de cooldown no catálogo.
   - Acionamento automático da rotação de bosses diários.

9. 🥋 **Treino nos Bonecos de Teste (`AutoTraining`):**
   - Quando o `AutoHunt` estiver pausado ou o personagem estiver descansando na cidade, aciona o treino online nos dummies (`training start online`).

10. 🎖️ **Promoção Automática (`AutoPromote`):**
    - Ao atingir o Level 20+, solicita automaticamente a promoção da vocação no templo (`promote`).

11. 👥 **Auto-Party & Multiboxing (`AutoParty`):**
    - Se múltiplos personagens estiverem no grupo, o líder puxa os companheiros para a mesma caçada (`meusChars puxar`).
    - Enforçamento de **Taxa Coletiva Anti-Ban** (`CalculatePartyMaxKillRate`) quando em grupo.

12. 🌐 **Multi-Contas com Multi-IP na VPS (`bindIP`):**
    - Cada conta em `accounts.json` pode especificar seu próprio IP local de saída (`bindIP`), isolando completamente o tráfego de rede por interface física/virtual.

13. 🚨 **Circuit Breaker Anti-Ban Integrado:**
    - Monitora respostas suspeitas, limites de requisições (429), conflito de concorrência (`sessionTaken`) e avisos do servidor.
    - Desarma combate imediatamente, recua para a cidade e aplica cooldown de proteção (15-30 min) antes de testar em half-open.

---

## 📁 Estrutura de Arquivos

```text
luxidlego/
├── accounts.example.json   # Modelo de configuração com exemplos de multi-IP e opções
├── accounts.json           # Suas credenciais reais (ignorado pelo Git)
├── AGENTS.md               # Diretrizes globais de anti-ban e engenharia
├── main.go                 # Ponto de entrada universal do motor
├── go.mod / go.sum         # Dependências Go (gorilla/websocket)
│
├── pkg/
│   ├── game/               # 🎮 Lógica do Lux Idle
│   │   ├── types.go        # Structs completas dos pacotes e estados
│   │   └── bot.go          # Implementação do engine.GameBot e automações
│   ├── engine/             # Circuit Breaker, supervisor e gerenciador
│   ├── network/            # WebSocket thread-safe com suporte a bindIP
│   └── server/             # Web dashboard e endpoints /api/status e /api/action
│
└── cmd/build/              # Script de cross-compilação para Linux/Windows
```

---

## ⚙️ Configuração (`accounts.json`)

Copie `accounts.example.json` para `accounts.json` e preencha suas contas:

```json
[
  {
    "id": "lux-1",
    "name": "Conta 1 (Principal)",
    "enabled": true,
    "bindIP": "",
    "credentials": {
      "email": "jogador1@exemplo.com",
      "password": "senha_da_conta",
      "characterName": "MeuGuerreiro"
    },
    "settings": {
      "huntTarget": "auto",
      "huntMode": "online",
      "strategy": "nearest",
      "distance": 0,
      "lure": 2,
      "autoHunt": true,
      "autoSell": true,
      "autoBless": true,
      "autoDaily": true,
      "autoTasks": true,
      "autoBoss": true,
      "autoTraining": true,
      "autoPromote": true,
      "autoParty": true,
      "autoAssist": true,
      "minHpPercent": 30
    }
  },
  {
    "id": "lux-2",
    "name": "Conta 2 (IP Secundário VPS)",
    "enabled": true,
    "bindIP": "10.0.0.2",
    "credentials": {
      "email": "jogador2@exemplo.com",
      "password": "senha_da_conta_2",
      "characterName": "MeuMago"
    },
    "settings": {
      "huntTarget": "auto",
      "huntMode": "online",
      "strategy": "lowest",
      "distance": 3,
      "lure": 0,
      "autoHunt": true,
      "autoSell": true,
      "autoBless": true,
      "autoDaily": true,
      "autoTasks": true,
      "autoBoss": true,
      "autoTraining": true,
      "autoPromote": true,
      "autoParty": true,
      "autoAssist": true,
      "minHpPercent": 35
    }
  }
]
```

---

## 🌐 Como Configurar Multi-IP na VPS Linux (Ubuntu / Debian)

Para associar múltiplos IPs públicos/privados na sua VPS para o bot:

1. Adicione os IPs secundários à interface de rede (ex: `eth0` ou `ens3`):
   ```bash
   sudo ip addr add 10.0.0.2/24 dev eth0
   sudo ip addr add 10.0.0.3/24 dev eth0
   ```
2. Configure o campo `"bindIP": "10.0.0.2"` em cada conta no `accounts.json`. O bot fará o binding nativo no nível do kernel (`net.Dialer.LocalAddr`), garantindo que todo o tráfego saia pelo IP selecionado.

---

## 🚀 Como Executar

### Localmente (Windows / Linux):
```bash
go run .
```
O console exibirá o ticker a cada 4 segundos com memória RAM, goroutines e status das contas, e o Web Dashboard estará acessível em `http://localhost:3001`.

### Compilar para VPS Linux (Ubuntu / Debian x86_64):
```bash
go run ./cmd/build linux
```
Ou via terminal:
```bash
cmd /c "set GOOS=linux&& set GOARCH=amd64&& go build -ldflags=\"-s -w\" -o luxidle-linux ."
```

Envie o binário para a VPS:
```bash
scp luxidle-linux ubuntu@seu-ip-vps:/home/ubuntu/luxidle/
```

Execute como serviço permanente com systemd:
```bash
sudo cp scripts/bot.service /etc/systemd/system/luxidle.service
sudo systemctl daemon-reload
sudo systemctl enable --now luxidle
```
