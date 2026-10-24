package game

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"bot-template-go/pkg/engine"
	"bot-template-go/pkg/network"
)

type Bot struct {
	config  AccountConfig
	mu      sync.RWMutex
	status  engine.AccountStatus
	socket  *network.SocketClient
	cb      *engine.CircuitBreaker

	// Estado dinâmico do personagem e da sessão
	character    *CharacterState
	hunt         *HuntState
	catalog      *CatalogData
	manager      *engine.BotManager
	activeChar   string
	sessionToken string

	// Controles temporais
	connectedAt  time.Time
	lastActionAt time.Time
	lastSellAt   time.Time
	lastDailyAt  time.Time
	lastBlessAt  time.Time
	lastTaskAt   time.Time
	lastBossAt   time.Time
	lastTrainAt  time.Time
	lastPreyAt   time.Time
	lastPartyAt  time.Time
	lastStateAt  time.Time
	lastBoxAt    time.Time
	lastMountAt  time.Time
	lastSkillsAt time.Time
	lastCharmsAt time.Time
	lastMarketAt time.Time

	// Novos ciclos automáticos
	lastEquipAt        time.Time
	lastBankTransferAt time.Time
	lastGemBagAt       time.Time
	lastPresetAt       time.Time
	monsterTasks       []MonsterTask

	// Estado cooperativo de party
	huntPartyFull bool
	knownInParty  map[string]bool

	// Canais de sinalização
	accountReady chan struct{}
	ticketChan   chan string
	discChan     chan error
}

func NewBot(cfg AccountConfig) *Bot {
	if !cfg.Settings.AutoEquip {
		cfg.Settings.AutoEquip = true
	}
	if !cfg.Settings.AutoGoldBank {
		cfg.Settings.AutoGoldBank = true
	}
	if cfg.Settings.MainAccount == "" {
		cfg.Settings.MainAccount = "kbelludoo"
	}

	st := engine.NewStatus(cfg.ID, cfg.Name)
	st.CurrentArea = cfg.Settings.HuntTarget
	if st.CurrentArea == "" {
		st.CurrentArea = "auto"
	}

	b := &Bot{
		config:       cfg,
		status:       st,
		catalog:      &CatalogData{},
		knownInParty: make(map[string]bool),
		accountReady: make(chan struct{}, 1),
		ticketChan:   make(chan string, 1),
		discChan:     make(chan error, 1),
	}

	// Configuração do Circuit Breaker de Anti-Cheat
	cbCfg := engine.DefaultCircuitBreakerConfig()
	cbCfg.OnTrip = func(reason string) {
		log.Printf("[%s] 🚨 CIRCUIT BREAKER DISPARADO: %s. Desarmando caça e recuando para a cidade!", b.ID(), reason)
		b.mu.Lock()
		b.config.Settings.AutoHunt = false
		b.status.Status = "SEGURANÇA (Circuit Breaker Ativo)"
		b.status.Logs = append(b.status.Logs, fmt.Sprintf("Anti-Cheat ativado (%s). Em repouso preventivo.", reason))
		if len(b.status.Logs) > 15 {
			b.status.Logs = b.status.Logs[1:]
		}
		b.mu.Unlock()

		// Envia stopHunt para recuar imediatamente para a cidade/templo seguro
		if b.socket != nil && b.socket.IsConnected() {
			_ = b.socket.SendJSON(map[string]any{"t": "stopHunt"})
		}
	}
	b.cb = engine.NewCircuitBreaker(cfg.ID, cbCfg)

	return b
}

func (b *Bot) ID() string {
	return b.config.ID
}

func (b *Bot) Name() string {
	return b.config.Name
}

func (b *Bot) SetManager(m *engine.BotManager) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.manager = m
}

func (b *Bot) Init(ctx context.Context) error {
	b.updateStatus(func(s *engine.AccountStatus) {
		s.Status = "Conectando ao Lux Idle..."
	})

	headers := make(http.Header)
	headers.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
	headers.Set("Origin", "https://luxidle.com.br")

	wsURL := "wss://luxidle.com.br/ws?livro=1&cacadas=1"
	b.socket = network.NewSocketClient(wsURL, headers, b.handleMessage, b.handleDisconnect)

	// Configura multi-IP se especificado no accounts.json
	if b.config.BindIP != "" {
		b.socket.SetBindIP(b.config.BindIP)
		log.Printf("[%s] Vinculado ao IP local da VPS: %s", b.ID(), b.config.BindIP)
	}

	return nil
}

func (b *Bot) Run(ctx context.Context) error {
	log.Printf("[%s] Abrindo conexão WebSocket com Lux Idle...", b.ID())
	if err := b.socket.Connect(); err != nil {
		return fmt.Errorf("falha ao conectar: %w", err)
	}
	defer b.socket.Close()

	b.connectedAt = time.Now()
	b.updateStatus(func(s *engine.AccountStatus) {
		s.Status = "Autenticando..."
	})

	// Handshake inicial do cliente Lux Idle
	_ = b.socket.SendJSON(map[string]any{"t": "delta", "on": true, "sessao": true, "fundo": true})
	_ = b.socket.SendJSON(map[string]any{"t": "oculta", "on": false})

	// Autenticação: Resume (se houver token) ou Login (email/senha)
	token := b.config.Credentials.SessionToken
	if token != "" {
		log.Printf("[%s] Retomando sessão com token existente...", b.ID())
		_ = b.socket.SendJSON(map[string]any{"t": "resume", "token": token})
	} else {
		log.Printf("[%s] Enviando credenciais de login para %s...", b.ID(), b.config.Credentials.Email)
		_ = b.socket.SendJSON(map[string]any{
			"t":        "login",
			"email":    b.config.Credentials.Email,
			"password": b.config.Credentials.Password,
		})
	}

	// Tickers de manutenção e loop de automação do jogo
	pingTicker := time.NewTicker(20 * time.Second)
	defer pingTicker.Stop()

	actionTicker := time.NewTicker(3 * time.Second)
	defer actionTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] Encerrando bot de forma limpa...", b.ID())
			if b.hunt != nil {
				_ = b.socket.SendJSON(map[string]any{"t": "stopHunt"})
			}
			return ctx.Err()

		case err := <-b.discChan:
			log.Printf("[%s] Desconexão detectada: %v. Reiniciando loop de conexão...", b.ID(), err)
			return fmt.Errorf("conexão perdida: %w", err)

		case <-pingTicker.C:
			if b.socket.IsConnected() {
				_ = b.socket.SendJSON(map[string]any{
					"t":  "ping",
					"at": time.Now().UnixMilli(),
				})
			}

		case <-actionTicker.C:
			if !b.socket.IsConnected() {
				return fmt.Errorf("socket desconectado")
			}
			b.performAutomationTick()
		}
	}
}

// -------------------------------------------------------------
// Tratamento de Mensagens do Jogo (Inbound Dispatcher)
// -------------------------------------------------------------

func (b *Bot) handleMessage(data []byte) {
	strData := string(data)

	// Verificação obrigatória do Circuit Breaker contra Anti-Cheat
	if rejected, reason := engine.IsAntiCheatRejection(200, strData); rejected {
		b.cb.Trip(fmt.Sprintf("anti-cheat detectado (%s): %s", reason, strData))
		return
	}

	var base BasePacket
	if err := json.Unmarshal(data, &base); err != nil {
		return
	}

	switch base.T {
	case "pong":
		// Keepalive confirmado pelo servidor

	case "hello":
		var p InboundPacket
		if err := json.Unmarshal(data, &p); err == nil && p.Catalog != nil {
			b.mergeCatalog(p.Catalog)
		}

	case "account":
		var p InboundPacket
		if err := json.Unmarshal(data, &p); err == nil {
			if p.Catalog != nil {
				b.mergeCatalog(p.Catalog)
			}
			b.handleAccountReceived(p)
		}

	case "welcome":
		var p InboundPacket
		if err := json.Unmarshal(data, &p); err != nil {
			log.Printf("[%s] ❌ Erro ao decodificar 'welcome': %v", b.ID(), err)
		} else {
			b.handleWelcomeReceived(p)
		}

	case "state":
		var p InboundPacket
		if err := json.Unmarshal(data, &p); err != nil {
			log.Printf("[%s] ❌ Erro ao decodificar 'state': %v", b.ID(), err)
		} else {
			b.handleStateReceived(p)
		}

	case "tasksDeBicho":
		b.handleTasksReceived(data)

	case "mounts":
		b.handleMountsReceived(data)

	case "arvore":
		b.handleArvoreReceived(data)

	case "charms":
		b.handleCharmsReceived(data)

	case "coinMarket":
		b.handleCoinMarketReceived(data)

	case "grupoConvite":
		var p struct {
			From string `json:"from"`
		}
		if err := json.Unmarshal(data, &p); err == nil && p.From != "" {
			log.Printf("[%s] 👥 Convite de party recebido de '%s'. Aceitando...", b.ID(), p.From)
			b.addLog(fmt.Sprintf("Aceitou convite de party de '%s'", p.From))
			_ = b.socket.SendJSON(map[string]any{
				"t":      "grupo",
				"action": "aceitar",
			})
		}

	case "partyInvite":
		var p struct {
			From string `json:"from"`
			Hunt string `json:"hunt"`
		}
		if err := json.Unmarshal(data, &p); err == nil && p.From != "" {
			// Se o membro ainda estiver abaixo do threshold da party, ignora para continuar o catch-up solo
			minRequiredLevel := 0
			if b.manager != nil && b.config.Settings.PartyLeader != "" {
				for _, st := range b.manager.GetAllStatuses() {
					if strings.EqualFold(st.Name, b.config.Settings.PartyLeader) {
						minRequiredLevel = (st.Level * 2) / 3
						break
					}
				}
			}
			if minRequiredLevel > 0 && b.character != nil && b.character.Level < minRequiredLevel {
				log.Printf("[%s] ⏳ Convite de caçada em '%s' de '%s' ignorado: personagem em catch-up (Level %d < %d)", b.ID(), p.Hunt, p.From, b.character.Level, minRequiredLevel)
				return
			}

			log.Printf("[%s] ⚔️ Convite de caçada em '%s' recebido de '%s'. Aceitando...", b.ID(), p.Hunt, p.From)
			b.addLog(fmt.Sprintf("Entrando na caçada da party com '%s' em '%s'", p.From, p.Hunt))
			_ = b.socket.SendJSON(map[string]any{
				"t":      "party",
				"action": "accept",
			})
		}

	case "pedidoDeEntrada":
		log.Printf("[%s] 👥 Pedido de entrada na caçada recebido. Aceitando...", b.ID())
		_ = b.socket.SendJSON(map[string]any{
			"t":      "party",
			"action": "aceitarPedido",
		})

	case "bloqueio":
		var p struct {
			Texto string `json:"texto"`
			Abre  int64  `json:"abre"`
		}
		_ = json.Unmarshal(data, &p)
		log.Printf("[%s] 🔒 Servidor em modo bloqueio / lançamento: %s", b.ID(), p.Texto)
		b.cb.Trip(fmt.Sprintf("servidor em bloqueio: %s", p.Texto))

	case "authError":
		var p InboundPacket
		_ = json.Unmarshal(data, &p)
		log.Printf("[%s] ⚠️ Erro de autenticação: %s (código: %s)", b.ID(), p.Message, p.Code)
		b.updateStatus(func(s *engine.AccountStatus) {
			s.Status = fmt.Sprintf("Erro de Auth: %s", p.Message)
		})
		b.cb.Trip(fmt.Sprintf("authError: %s", p.Message))

	case "sessionTaken":
		log.Printf("[%s] ⚠️ Conta aberta em outra aba/instância (sessionTaken). Parando bot.", b.ID())
		b.cb.Trip("sessionTaken: conflito de concorrência")

	case "error":
		var p InboundPacket
		_ = json.Unmarshal(data, &p)
		msg := p.Message
		log.Printf("[%s] Aviso do jogo: %s", b.ID(), msg)
		b.addLog(fmt.Sprintf("Aviso: %s", msg))

		b.mu.Lock()
		if strings.Contains(msg, "já está numa party") {
			parts := strings.Split(msg, " ")
			if len(parts) > 0 {
				b.knownInParty[strings.ToLower(parts[0])] = true
			}
		}
		if strings.Contains(msg, "já tem 5 pessoas") || strings.Contains(msg, "cheia") {
			b.huntPartyFull = true
		}
		b.mu.Unlock()
	}
}

func (b *Bot) handleDisconnect(err error) {
	log.Printf("[%s] Conexão WebSocket perdida: %v", b.ID(), err)
	b.updateStatus(func(s *engine.AccountStatus) {
		s.Status = "Desconectado. Reconectando..."
	})
	select {
	case b.discChan <- err:
	default:
	}
}

// -------------------------------------------------------------
// Lógica de Autenticação e Entrada no Jogo
// -------------------------------------------------------------

func (b *Bot) resolveVocation() string {
	if b.config.Credentials.Vocation != "" {
		return strings.ToLower(b.config.Credentials.Vocation)
	}
	nameLower := strings.ToLower(b.config.Name)
	if strings.Contains(nameLower, "knight") {
		return "knight"
	} else if strings.Contains(nameLower, "druid") {
		return "druid"
	} else if strings.Contains(nameLower, "sorcerer") {
		return "sorcerer"
	} else if strings.Contains(nameLower, "paladin") || strings.Contains(nameLower, "pally") {
		return "paladin"
	} else if strings.Contains(nameLower, "monk") {
		return "monk"
	}
	return "knight"
}

func (b *Bot) handleAccountReceived(p InboundPacket) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if p.Token != "" {
		b.sessionToken = p.Token
		b.config.Credentials.SessionToken = p.Token
	}

	if p.Account == nil || len(p.Account.Characters) == 0 {
		targetChar := b.config.Credentials.CharacterName
		if targetChar != "" {
			voc := b.resolveVocation()
			log.Printf("[%s] 👶 Conta sem personagens após lançamento oficial! Criando automaticamente '%s' (%s)...", b.ID(), targetChar, voc)
			b.status.Status = fmt.Sprintf("Criando '%s' (%s)...", targetChar, voc)
			_ = b.socket.SendJSON(map[string]any{
				"t":        "createCharacter",
				"name":     targetChar,
				"vocation": voc,
				"sex":      "male",
			})
			return
		}
		log.Printf("[%s] ⚠️ Nenhum personagem encontrado nesta conta!", b.ID())
		b.status.Status = "Sem personagens na conta"
		return
	}

	// Seleciona personagem configurado ou o primeiro disponível
	targetChar := b.config.Credentials.CharacterName
	chosenChar := p.Account.Characters[0].Name

	if targetChar != "" {
		for _, c := range p.Account.Characters {
			if strings.EqualFold(c.Name, targetChar) {
				chosenChar = c.Name
				break
			}
		}
	}

	b.activeChar = chosenChar
	log.Printf("[%s] Personagem selecionado: %s. Enviando pacote 'play'...", b.ID(), chosenChar)

	// Envia pacote play para entrar no mundo
	_ = b.socket.SendJSON(map[string]any{
		"t":     "play",
		"name":  chosenChar,
		"visto": time.Now().UnixMilli(),
	})
}

func (b *Bot) handleWelcomeReceived(p InboundPacket) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if p.Catalog != nil {
		b.mergeCatalogLocked(p.Catalog)
	}

	if p.Character != nil {
		b.character = p.Character
		b.status.Name = p.Character.Name
		b.status.Level = p.Character.Level
		b.status.Vocation = p.Character.Vocation
		b.status.Gold = p.Character.Gold
		b.status.HP = p.Character.HP
		b.status.Mana = p.Character.Mana
		b.status.Stamina = p.Character.GetStaminaMinutes()

		if p.Character.Derived != nil {
			b.status.MaxHP = p.Character.Derived.MaxHP
			b.status.MaxMana = p.Character.Derived.MaxMana
		}
	}

	if p.Hunt != nil {
		b.hunt = p.Hunt
		b.status.Status = "hunting"
		b.status.CurrentArea = p.Hunt.HuntID
	} else {
		b.hunt = nil
		b.status.Status = "idle (cidade)"
	}

	b.status.Logs = append(b.status.Logs, fmt.Sprintf("Entrou no jogo como %s (Level %d %s)", b.status.Name, b.status.Level, b.status.Vocation))
	log.Printf("[%s] Bem-vindo ao mundo! %s (Level %d %s) pronto para a ação.", b.ID(), b.status.Name, b.status.Level, b.status.Vocation)

	// Configurações de inicialização do cliente oficial
	_ = b.socket.SendJSON(map[string]any{"t": "fuso", "tz": "America/Sao_Paulo"})
	_ = b.socket.SendJSON(map[string]any{"t": "settings", "autoPronto": true})
}

func (b *Bot) handleStateReceived(p InboundPacket) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.lastStateAt = time.Now()

	// 1. Atualizar personagem (Full ou Delta)
	if p.Character != nil {
		if b.character == nil || !p.CharDelta {
			b.character = p.Character
		} else {
			// Merge delta
			if p.Character.Level > 0 {
				b.character.Level = p.Character.Level
			}
			if p.Character.HP > 0 {
				b.character.HP = p.Character.HP
			}
			if p.Character.Mana > 0 {
				b.character.Mana = p.Character.Mana
			}
			if p.Character.Gold > 0 {
				b.character.Gold = p.Character.Gold
			}
			if p.Character.Coins > 0 {
				b.character.Coins = p.Character.Coins
			}
			if p.Character.Mount > 0 {
				b.character.Mount = p.Character.Mount
			}
			if p.Character.Derived != nil {
				b.character.Derived = p.Character.Derived
			}
			if p.Character.Progress != nil {
				b.character.Progress = p.Character.Progress
			}
			if p.Character.Party != nil {
				b.character.Party = p.Character.Party
			}
			if p.Character.Lobby != nil {
				b.character.Lobby = p.Character.Lobby
			}
			if p.Character.BossCooldowns != nil {
				b.character.BossCooldowns = p.Character.BossCooldowns
			}
			if len(p.Character.Prey) > 0 {
				b.character.Prey = p.Character.Prey
			}
			if p.Character.Stamina != nil {
				b.character.Stamina = p.Character.Stamina
			}
			if p.Character.MaxStamina != nil {
				b.character.MaxStamina = p.Character.MaxStamina
			}
			if p.Character.TreinoStamina != nil {
				b.character.TreinoStamina = p.Character.TreinoStamina
			}
			if p.Character.Wildcards > 0 {
				b.character.Wildcards = p.Character.Wildcards
			}
			if p.Character.ArvorePontos != nil {
				b.character.ArvorePontos = p.Character.ArvorePontos
			}
			if p.Character.CharmPoints > 0 {
				b.character.CharmPoints = p.Character.CharmPoints
			}
			if len(p.Character.Inventory) > 0 {
				b.character.Inventory = p.Character.Inventory
			}
			if p.Character.Equipment != nil {
				b.character.Equipment = p.Character.Equipment
			}
		}

		b.status.Name = b.character.Name
		b.status.Level = b.character.Level
		b.status.Vocation = b.character.Vocation
		b.status.HP = b.character.HP
		b.status.Mana = b.character.Mana
		b.status.Gold = b.character.Gold
		b.status.Stamina = b.character.GetStaminaMinutes()

		if b.character.Derived != nil {
			b.status.MaxHP = b.character.Derived.MaxHP
			b.status.MaxMana = b.character.Derived.MaxMana
		}
	}

	// 2. Atualizar estado de Hunt (Caçada)
	if p.Hunt != nil {
		if b.hunt == nil || !p.HuntDelta {
			b.hunt = p.Hunt
		} else {
			if p.Hunt.HuntID != "" {
				b.hunt.HuntID = p.Hunt.HuntID
			}
			if p.Hunt.Session != nil {
				if b.hunt.Session == nil || !p.SessaoDelta {
					b.hunt.Session = p.Hunt.Session
				} else {
					if p.Hunt.Session.Kills > 0 {
						b.hunt.Session.Kills = p.Hunt.Session.Kills
					}
					if p.Hunt.Session.Exp > 0 {
						b.hunt.Session.Exp = p.Hunt.Session.Exp
					}
					if p.Hunt.Session.Gold > 0 {
						b.hunt.Session.Gold = p.Hunt.Session.Gold
					}
				}
			}
		}

		b.status.Status = "hunting"
		b.status.CurrentArea = b.hunt.HuntID

		// Se a caçada atual estiver em modo manual, encerra para reiniciar em modo ciclo (automático)
		if b.hunt.Manual && b.config.Settings.AutoHunt {
			log.Printf("[%s] ⚠️ Caçada manual detectada! Parando caçada manual para reiniciar em modo ciclo (automático)...", b.ID())
			b.addLog("Saindo de caçada manual para entrar em modo automático.")
			_ = b.socket.SendJSON(map[string]any{"t": "stopHunt"})
		}

		if b.hunt.Session != nil {
			b.status.Kills = b.hunt.Session.Kills
			// Calcula XP/h com base no tempo de sessão
			durationHours := time.Since(b.connectedAt).Hours()
			if durationHours > 0.005 && b.hunt.Session.Exp > 0 {
				b.status.ExpPerHour = float64(b.hunt.Session.Exp) / durationHours
			}
		}
	} else if !p.HuntDelta && p.Character != nil {
		// Se não veio hunt e não é delta de hunt, estamos na cidade/parados
		b.hunt = nil
		b.huntPartyFull = false
		if b.status.Status == "Autenticando..." || b.status.Status == "hunting" || b.status.Status == "" {
			b.status.Status = "idle (cidade)"
		}
	}

	b.status.UpdatedAt = time.Now().UnixMilli()
	b.status.DurationSec = int(time.Since(b.connectedAt).Seconds())
}

func (b *Bot) handleTasksReceived(data []byte) {
	// Trata tasks de monstros entregues ou pendentes
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return
	}

	var list []any
	if f, ok := raw["ficha"].([]any); ok {
		list = f
	} else if t, ok := raw["tasks"].([]any); ok {
		list = t
	}

	if len(list) == 0 {
		return
	}

	b.mu.RLock()
	currentHuntID := ""
	if b.hunt != nil {
		currentHuntID = b.hunt.HuntID
	}
	b.mu.RUnlock()

	// Identifica criaturas da caçada atual a partir do catálogo
	huntCreatures := make(map[string]bool)
	if b.catalog != nil && currentHuntID != "" {
		for _, h := range b.catalog.Hunts {
			if h.ID == currentHuntID {
				for _, c := range h.Creatures {
					if cm, ok := c.(map[string]any); ok {
						if k, ok := cm["key"].(string); ok {
							huntCreatures[k] = true
						}
					}
				}
				break
			}
		}
	}

	for _, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		key, _ := m["key"].(string)
		if key == "" {
			continue
		}

		aResgatar, _ := m["aResgatar"].(float64)
		completa, _ := m["completa"].(bool)
		pausada, _ := m["pausada"].(bool)
		feito, _ := m["feito"].(bool)

		if aResgatar > 0 || completa || feito {
			log.Printf("[%s] 📜 AutoTasks: Resgatando recompensa de task '%s'...", b.ID(), key)
			b.addLog(fmt.Sprintf("Resgatando prêmio da task '%s'", key))
			_ = b.socket.SendJSON(map[string]any{
				"t":      "tasksDeBicho",
				"action": "resgatar",
				"key":    key,
			})
			time.Sleep(250 * time.Millisecond)
		} else if pausada && huntCreatures[key] {
			log.Printf("[%s] 📜 AutoTasks: Aceitando task de '%s' para a caçada atual!", b.ID(), key)
			b.addLog(fmt.Sprintf("Aceitou task de '%s'", key))
			_ = b.socket.SendJSON(map[string]any{
				"t":      "tasksDeBicho",
				"action": "aceitar",
				"key":    key,
			})
			time.Sleep(250 * time.Millisecond)
		}
	}
}

// -------------------------------------------------------------
// Loop de Automação do Jogo (Ações Inteligentes e Seguras)
// -------------------------------------------------------------

func (b *Bot) performAutomationTick() {
	// 1. Respeitar o Circuit Breaker
	if !b.cb.AllowAction() {
		return
	}

	b.mu.RLock()
	char := b.character
	hunt := b.hunt
	cfg := b.config.Settings
	b.mu.RUnlock()

	now := time.Now()

	if char == nil || char.Name == "" {
		if time.Since(b.connectedAt) > 15*time.Second && b.activeChar != "" {
			if now.Sub(b.lastActionAt) > 15*time.Second {
				b.lastActionAt = now
				log.Printf("[%s] Aguardando confirmação do servidor... reenviando 'play' para '%s'", b.ID(), b.activeChar)
				_ = b.socket.SendJSON(map[string]any{
					"t":     "play",
					"name":  b.activeChar,
					"visto": time.Now().UnixMilli(),
				})
			}
		}
		return // Aguardando welcome/state do servidor
	}


	// 2. Proteção de Vida Crítica (Emergency Retreat)
	if hunt != nil && char.HP > 0 && char.Derived != nil && char.Derived.MaxHP > 0 {
		minPercent := cfg.MinHPPercent
		if minPercent <= 0 {
			minPercent = 30
		}
		currentPercent := (char.HP * 100) / char.Derived.MaxHP
		if currentPercent < minPercent {
			log.Printf("[%s] ⚠️ HP Crítico (%d/%d - %d%%). Saindo da hunt para recuperar vida!", b.ID(), char.HP, char.Derived.MaxHP, currentPercent)
			b.addLog(fmt.Sprintf("HP Crítico (%d%%). Recuando para a cidade.", currentPercent))
			_ = b.socket.SendJSON(map[string]any{"t": "stopHunt"})
			return
		}
	}

	// 3. Promoção Automática de Vocação (Level 20+)
	if cfg.AutoPromote && char.Level >= 20 && !strings.Contains(char.Vocation, "Elite") && !strings.Contains(char.Vocation, "Master") && !strings.Contains(char.Vocation, "Elder") && !strings.Contains(char.Vocation, "Royal") {
		if now.Sub(b.lastActionAt) > 60*time.Second {
			b.lastActionAt = now
			log.Printf("[%s] Promovendo vocação no level %d...", b.ID(), char.Level)
			_ = b.socket.SendJSON(map[string]any{"t": "promote"})
		}
	}

	// 4. Bênçãos Automáticas (AutoBless)
	if cfg.AutoBless && char.Level >= 20 && now.Sub(b.lastBlessAt) > 5*time.Minute {
		b.lastBlessAt = now
		_ = b.socket.SendJSON(map[string]any{"t": "bless", "all": true})
	}

	// 5. Recompensas Diárias e Quests (AutoDaily)
	if cfg.AutoDaily && now.Sub(b.lastDailyAt) > 15*time.Minute {
		b.lastDailyAt = now
		log.Printf("[%s] Resgatando recompensas diárias e passe de elite...", b.ID())
		_ = b.socket.SendJSON(map[string]any{"t": "reward", "action": "takeAll"})
		_ = b.socket.SendJSON(map[string]any{"t": "diario"})
		_ = b.socket.SendJSON(map[string]any{"t": "passe", "action": "resgatarTudo"})
	}

	// 6. Limpeza, Venda de Inventário, Abertura de Caixas de Boss e Aljava
	if now.Sub(b.lastSellAt) > 2*time.Minute {
		b.lastSellAt = now
		if cfg.AutoSell {
			_ = b.socket.SendJSON(map[string]any{"t": "venderSacolas"})
			_ = b.socket.SendJSON(map[string]any{"t": "venderMochila"})
			_ = b.socket.SendJSON(map[string]any{"t": "clearPouch"})
		}
		b.checkAndHandleBoxesAndQuiver()
	}

	// 7. Tasks de Monstros (AutoTasks)
	if cfg.AutoTasks && now.Sub(b.lastTaskAt) > 3*time.Minute {
		b.lastTaskAt = now
		_ = b.socket.SendJSON(map[string]any{"t": "tasksDeBicho"})
	}

	// 8. Rotação de Bosses Automática (AutoBoss)
	if cfg.AutoBoss && hunt == nil && now.Sub(b.lastBossAt) > 10*time.Minute {
		b.lastBossAt = now
		b.checkAndTriggerBoss()
	}

	// 9. Caçada Automática (AutoHunt) vs Treino nos Dummies / Regeneração de Stamina
	staminaMin := char.GetStaminaMinutes()

	// Se a stamina estiver zerada (0 min), para caçada e treina para regenerar até 12 horas
	if staminaMin <= 0 && cfg.AutoHunt {
		// 1. Tenta usar extensão de stamina caso disponível na mochila
		_ = b.socket.SendJSON(map[string]any{
			"t":    "autoStamina",
			"acao": "usar",
			"onde": "mochila",
		})

		// 2. Se estiver caçando, encerra a caçada para evitar 0 de XP
		if hunt != nil {
			log.Printf("[%s] ⏳ Stamina esgotada (0 min)! Parando caçada para regenerar...", b.ID())
			b.addLog("Stamina zerada! Saindo da caçada para descansar e treinar.")
			_ = b.socket.SendJSON(map[string]any{"t": "stopHunt"})
			return
		}

		// 3. Na cidade: Inicia treino para evoluir perícias enquanto a stamina regenera
		if now.Sub(b.lastTrainAt) > 2*time.Minute {
			b.lastTrainAt = now
			skill := b.resolveTrainingSkill(char.Vocation)
			log.Printf("[%s] 🥋 Stamina zerada: Regenerando stamina enquanto treina '%s'...", b.ID(), skill)
			b.addLog(fmt.Sprintf("Regenerando stamina enquanto treina (%s).", skill))
			_ = b.socket.SendJSON(map[string]any{
				"t":      "training",
				"action": "start",
				"mode":   "online",
			})
			b.updateStatus(func(s *engine.AccountStatus) {
				s.Status = "treinando / regenerando stamina"
				s.CurrentArea = "Pátio de Treino"
			})
		}
		return
	}

	if cfg.AutoHunt {
		isLeader := (cfg.PartyLeader != "" && strings.EqualFold(char.Name, cfg.PartyLeader)) ||
			(char.Party != nil && char.Party.SouLider)

		inParty := cfg.AutoParty && (
			(char.Party != nil && len(char.Party.Membros) > 1) ||
			(hunt != nil && hunt.Party != nil && len(hunt.Party.Membros) > 1) ||
			(cfg.PartyLeader != ""))

		if inParty {
			// Cenário 1: Operação em Grupo / Party Cooperativa
			if isLeader {
				// Líder coordena o Lobby e a migração de mapa da party
				targetHuntID := b.resolveBestHunt(char.Level)

				// Verifica se há membros que precisam de catch-up de level para partilha de EXP (minLevel >= floor(maxLevel * 2/3))
				minRequiredLevel := (char.Level * 2) / 3
				convocados := make([]string, 0)
				allMembersReady := true

				if b.manager != nil {
					statuses := b.manager.GetAllStatuses()
					for _, pm := range char.Party.Membros {
						if strings.EqualFold(pm.Name, char.Name) {
							continue
						}
						memberLevel := 0
						for _, st := range statuses {
							if strings.EqualFold(st.Name, pm.Name) {
								memberLevel = st.Level
								break
							}
						}
						if memberLevel > 0 && memberLevel < minRequiredLevel {
							allMembersReady = false
						} else {
							convocados = append(convocados, pm.Name)
						}
					}
				} else {
					for _, pm := range char.Party.Membros {
						if !strings.EqualFold(pm.Name, char.Name) {
							convocados = append(convocados, pm.Name)
						}
					}
				}

				if !allMembersReady && len(convocados) == 0 {
					// Membros ainda estão em catch-up solo, líder aguarda para não inflar o nível
					if hunt == nil && now.Sub(b.lastTrainAt) > 2*time.Minute {
						b.lastTrainAt = now
						log.Printf("[%s] ⏳ Membros em catch-up (nível mínimo requerido: %d). Líder treinando online nos bonecos...", b.ID(), minRequiredLevel)
						_ = b.socket.SendJSON(map[string]any{"t": "training", "action": "start", "mode": "online"})
					}
				} else if hunt == nil {
					// Time apto e não está caçando: abre o lobby se ainda não estiver aberto
					if char.Lobby == nil && len(convocados) > 0 && now.Sub(b.lastPartyAt) > 10*time.Second {
						b.lastPartyAt = now
						token := generateActionToken("lobby:" + targetHuntID)
						log.Printf("[%s] 🏰 Armando Lobby oficial para '%s' com %d aliados convocados...", b.ID(), targetHuntID, len(convocados))
						b.addLog(fmt.Sprintf("Armando Lobby da party em '%s'", targetHuntID))
						_ = b.socket.SendJSON(map[string]any{
							"t":          "lobby",
							"action":     "armar",
							"huntId":     targetHuntID,
							"modo":       "auto",
							"convocados": convocados,
							"acao":       token,
						})
					}
				} else {
					// Já está caçando com a party. Verifica se a party pode migrar para um mapa superior
					if now.Sub(b.lastActionAt) > 30*time.Second {
						if targetHuntID != "" && hunt.HuntID != "" && hunt.HuntID != targetHuntID {
							log.Printf("[%s] 🚀 Level UP da party! Migrando de '%s' para '%s'...", b.ID(), hunt.HuntID, targetHuntID)
							b.addLog(fmt.Sprintf("Migrando party de '%s' para '%s'", hunt.HuntID, targetHuntID))
							b.lastActionAt = now
							_ = b.socket.SendJSON(map[string]any{"t": "stopHunt"})
						}
					}
				}
			} else {
				// Membro da party
				leaderLevel := 0
				leaderArea := ""
				if b.manager != nil && cfg.PartyLeader != "" {
					for _, st := range b.manager.GetAllStatuses() {
						if strings.EqualFold(st.Name, cfg.PartyLeader) {
							leaderLevel = st.Level
							leaderArea = st.CurrentArea
							break
						}
					}
				}
				minRequiredLevel := 0
				if leaderLevel > 0 {
					minRequiredLevel = (leaderLevel * 2) / 3
				}

				if minRequiredLevel > 0 && char.Level < minRequiredLevel {
					// Nível insuficiente para divisão de EXP da party: faz catch-up solo em modo ciclo!
					if hunt == nil {
						log.Printf("[%s] ⚡ Catch-Up Solo: Nível %d < %d (threshold da party). Caçando solo no melhor mapa...", b.ID(), char.Level, minRequiredLevel)
						b.startSafeHunt()
					}
				} else {
					// Nível compatível para shared exp!
					if hunt == nil {
						// Se não está caçando e o líder está em caçada, junta-se na mesma caçada
						if leaderArea != "" {
							log.Printf("[%s] ⚔️ Aliado apto para a party (%d >= %d). Entrando na caçada com o líder em '%s'...", b.ID(), char.Level, minRequiredLevel, leaderArea)
							token := generateActionToken(fmt.Sprintf("startHunt:%s:cycle", leaderArea))
							_ = b.socket.SendJSON(map[string]any{
								"t":        "startHunt",
								"huntId":   leaderArea,
								"strategy": cfg.Strategy,
								"mode":     "cycle",
								"ids":      []string{leaderArea},
								"acao":     token,
								"limites":  map[string]any{},
							})
						}
					} else {
						// Membro já está na caçada da party: permanece com o time, nunca migra individualmente!
					}
				}
			}
		} else {
			// Cenário 2: Caçada Solo (sem party ou party ainda sendo montada)
			if hunt == nil {
				b.startSafeHunt()
			} else {
				if now.Sub(b.lastActionAt) > 30*time.Second {
					bestMap := b.resolveBestHunt(char.Level)
					if bestMap != "" && hunt.HuntID != "" && hunt.HuntID != bestMap {
						log.Printf("[%s] 🚀 Level UP ou mapa desatualizado! Migrando de '%s' para '%s'...", b.ID(), hunt.HuntID, bestMap)
						b.addLog(fmt.Sprintf("Migrando caçada de '%s' para '%s'", hunt.HuntID, bestMap))
						b.lastActionAt = now
						_ = b.socket.SendJSON(map[string]any{"t": "stopHunt"})
						time.Sleep(500 * time.Millisecond)
						b.startSafeHunt()
						return
					}
				}
			}
		}

		// Garante configurações de assistência e combate
		if hunt != nil && cfg.AutoAssist {
			_ = b.socket.SendJSON(map[string]any{"t": "huntAssist", "tipo": "ataque", "on": true})
			_ = b.socket.SendJSON(map[string]any{"t": "huntAssist", "tipo": "barra", "on": true})
		}
	} else if cfg.AutoTraining {
		// Transição Automática: Se for o Knight esperando a party atingir o level da exp compartilhada (148)
		if char.Level >= 200 && b.manager != nil {
			allReady := true
			otherCount := 0
			for _, st := range b.manager.GetAllStatuses() {
				if st.ID == b.ID() {
					continue
				}
				otherCount++
				if st.Level < 148 {
					allReady = false
					break
				}
			}
			if otherCount > 0 && allReady {
				log.Printf("[%s] 🎉 TODOS OS MEMBROS ATINGIRAM NÍVEL 148+! Ativando caçada em grupo do Knight...", b.ID())
				b.addLog("Todos membros nível 148+! Saindo dos dummies para caçar em grupo!")
				b.mu.Lock()
				b.config.Settings.AutoTraining = false
				b.config.Settings.AutoHunt = true
				b.config.Settings.AutoParty = true
				b.mu.Unlock()
				_ = b.socket.SendJSON(map[string]any{"t": "training", "action": "stop"})
				b.startSafeHunt()
				return
			}
		}

		// Se AutoHunt estiver desligado e AutoTraining ativado (ex: Knight aguardando a party)
		if hunt != nil {
			log.Printf("[%s] Parando caçada para ir ao Pátio dos Dummies treinar skills...", b.ID())
			b.addLog("Saindo da caçada para treinar nos dummies.")
			_ = b.socket.SendJSON(map[string]any{"t": "stopHunt"})
		}
		if now.Sub(b.lastTrainAt) > 2*time.Minute {
			b.lastTrainAt = now
			log.Printf("[%s] 🥋 Treinando online nos bonecos de teste (Target Dummies)...", b.ID())
			b.addLog("Treinando online nos Target Dummies.")
			_ = b.socket.SendJSON(map[string]any{
				"t":      "training",
				"action": "start",
				"mode":   "online",
			})
			b.updateStatus(func(s *engine.AccountStatus) {
				s.Status = "training (dummies)"
				s.CurrentArea = "Pátio dos Dummies"
			})
		}
	}

	// 10. Sistema de Presas (AutoPrey)
	if cfg.AutoPrey && now.Sub(b.lastPreyAt) > 1*time.Minute {
		b.lastPreyAt = now
		b.checkAndHandlePrey()
	}

	// 11. AutoParty e Sincronização de Caçada em Grupo
	if cfg.AutoParty && char != nil {
		isLeader := (cfg.PartyLeader != "" && strings.EqualFold(char.Name, cfg.PartyLeader)) ||
			(char.Party != nil && char.Party.SouLider)

		partyLeader := ""
		if char.Party != nil {
			partyLeader = char.Party.GetLeader()
		}

		// Se estamos numa party cujo líder NÃO é o líder configurado e nem nós mesmos, saímos
		if !isLeader && char.Party != nil && len(char.Party.Membros) > 0 && cfg.PartyLeader != "" {
			if partyLeader != "" && !strings.EqualFold(partyLeader, cfg.PartyLeader) {
				if now.Sub(b.lastPartyAt) > 30*time.Second {
					b.lastPartyAt = now
					log.Printf("[%s] 👥 Saindo da party antiga ('%s') para aguardar convite de '%s'...", b.ID(), partyLeader, cfg.PartyLeader)
					b.addLog(fmt.Sprintf("Saindo da party antiga (%s).", partyLeader))
					_ = b.socket.SendJSON(map[string]any{
						"t":      "grupo",
						"action": "sair",
					})
				}
			}
		}

		if isLeader && now.Sub(b.lastPartyAt) > 15*time.Second {
			b.lastPartyAt = now

			inParty := make(map[string]bool)
			for k, v := range b.knownInParty {
				if v {
					inParty[k] = true
				}
			}
			if char.Party != nil {
				for _, m := range char.Party.Membros {
					if m.Name != "" {
						inParty[strings.ToLower(m.Name)] = true
					}
				}
			}

			// Se a caçada atual já estiver com 5 membros, evita spam de invite
			huntFull := b.huntPartyFull
			if hunt != nil && hunt.Party != nil && len(hunt.Party.Membros) >= 5 {
				huntFull = true
			}

			for _, memberName := range cfg.PartyMembers {
				if strings.EqualFold(memberName, char.Name) {
					continue
				}
				if !inParty[strings.ToLower(memberName)] {
					log.Printf("[%s] 👥 AutoParty: Convidando '%s' para a party...", b.ID(), memberName)
					_ = b.socket.SendJSON(map[string]any{
						"t":      "grupo",
						"action": "convidar",
						"name":   memberName,
					})
				} else if hunt != nil && !huntFull {
					// Só convida para a caçada se o aliado já atingiu o threshold de shared EXP da party
					memberLevel := 0
					if b.manager != nil {
						for _, st := range b.manager.GetAllStatuses() {
							if strings.EqualFold(st.Name, memberName) {
								memberLevel = st.Level
								break
							}
						}
					}
					minRequiredLevel := (char.Level * 2) / 3
					if memberLevel >= minRequiredLevel || memberLevel == 0 {
						_ = b.socket.SendJSON(map[string]any{
							"t":      "party",
							"action": "invite",
							"name":   memberName,
						})
					}
				}
			}
		}

		// Automação cooperativa de Lobby oficial
		if char.Lobby != nil {
			if char.Lobby.SouLider {
				if char.Lobby.Frente == "" {
					_ = b.socket.SendJSON(map[string]any{
						"t":      "lobby",
						"action": "frente",
						"name":   "kbelludoo",
					})
				}
				// Configura formação para membros que estão atrás do knight
				for _, m := range char.Lobby.Membros {
					if !m.Lider && !strings.EqualFold(m.Nome, "kbelludoo") {
						_ = b.socket.SendJSON(map[string]any{
							"t":          "lobby",
							"action":     "formacao",
							"name":       m.Nome,
							"seguirQuem": "kbelludoo",
							"coleira":    3,
						})
					}
				}
				if char.Lobby.PodeComecar {
					log.Printf("[%s] 🏰 Time pronto no lobby! Iniciando caçada cooperativa (+100%% EXP)...", b.ID())
					b.addLog("Time pronto no lobby. Entrando na caçada cooperativa.")
					_ = b.socket.SendJSON(map[string]any{
						"t":       "lobby",
						"action":  "comecar",
						"limites": map[string]any{},
					})
				}
			} else {
				isReady := false
				for _, m := range char.Lobby.Membros {
					if m.Eu && m.Pronto {
						isReady = true
						break
					}
				}
				if !isReady {
					log.Printf("[%s] 🏰 Convocado para o lobby pelo líder! Confirmando 'Pronto'...", b.ID())
					b.addLog("Convocado para o lobby. Confirmando pronto.")
					_ = b.socket.SendJSON(map[string]any{
						"t":      "lobby",
						"action": "pronto",
					})
				}
			}
		}
	}

	// 12. Montaria Automática (+Velocidade)
	if now.Sub(b.lastMountAt) > 5*time.Minute {
		b.lastMountAt = now
		b.checkAndHandleMount()
	}

	// 13. Árvore de Habilidades (AutoSkills)
	if now.Sub(b.lastSkillsAt) > 3*time.Minute {
		b.lastSkillsAt = now
		b.checkAndHandleSkills()
	}

	// 14. Bestiário e Charms (AutoCharms)
	if now.Sub(b.lastCharmsAt) > 10*time.Minute {
		b.lastCharmsAt = now
		b.checkAndHandleCharms()
	}

	// 15. Cotação do Mercado de Lux Coins (CoinMarket)
	if now.Sub(b.lastMarketAt) > 15*time.Minute {
		b.lastMarketAt = now
		b.checkAndHandleCoinMarket()
	}

	// 16. AutoEquip Inteligente de Upgrades de Armaduras, Armas e Escudos
	if cfg.AutoEquip && now.Sub(b.lastEquipAt) > 1*time.Minute {
		b.lastEquipAt = now
		b.checkAndHandleEquipment()
	}

	// 17. Centralização Automática de Ouro / Banco para a Conta Principal
	if cfg.AutoGoldBank && now.Sub(b.lastBankTransferAt) > 5*time.Minute {
		b.lastBankTransferAt = now
		b.checkAndHandleGoldTransfer()
	}

	// 18. Gem Bag e Armazenamento de Gemas
	if now.Sub(b.lastGemBagAt) > 5*time.Minute {
		b.lastGemBagAt = now
		b.checkAndHandleGemBag()
	}

	// 19. Action Presets e Rotação de Magias
	if now.Sub(b.lastPresetAt) > 10*time.Minute {
		b.lastPresetAt = now
		b.checkAndApplyActionPresets()
	}
}

func (b *Bot) checkAndHandleBoxesAndQuiver() {
	b.mu.RLock()
	char := b.character
	b.mu.RUnlock()

	if char == nil {
		return
	}

	// Consulta e abre bolsas de recompensas do jogo
	_ = b.socket.SendJSON(map[string]any{"t": "bossPouch"})
	_ = b.socket.SendJSON(map[string]any{"t": "storeInbox"})

	for _, item := range char.Inventory {
		isBox := item.Tipo == "caixa" ||
			strings.Contains(strings.ToLower(item.Name), "caixa") ||
			strings.Contains(strings.ToLower(item.Name), "box") ||
			strings.Contains(strings.ToLower(item.Name), "bolsa") ||
			strings.Contains(strings.ToLower(item.Name), "presente")
		itemID := item.GetID()
		if isBox && itemID != "" && itemID != "0" {
			_ = b.socket.SendJSON(map[string]any{
				"t":  "abrirTodas",
				"id": item.ID,
			})
		}

		// Aljava para Paladino
		if strings.Contains(strings.ToLower(char.Vocation), "paladin") {
			isAmmo := item.Slot == "ammo" || item.Ammo ||
				strings.Contains(strings.ToLower(item.Name), "arrow") ||
				strings.Contains(strings.ToLower(item.Name), "bolt")
			if isAmmo && itemID != "" && itemID != "0" {
				_ = b.socket.SendJSON(map[string]any{
					"t":       "aljavaGuardar",
					"id":      item.ID,
					"guardar": true,
				})
			}
		}
	}
}

func (b *Bot) checkAndHandleMount() {
	_ = b.socket.SendJSON(map[string]any{"t": "mounts"})
}

func (b *Bot) handleMountsReceived(data []byte) {
	var p struct {
		Mounts []struct {
			ID    int    `json:"id"`
			Name  string `json:"name"`
			Owned bool   `json:"owned"`
		} `json:"mounts"`
	}
	if err := json.Unmarshal(data, &p); err != nil {
		return
	}

	for _, m := range p.Mounts {
		if m.Owned {
			b.mu.Lock()
			currentMount := 0
			if b.character != nil {
				currentMount = b.character.Mount
			}
			b.mu.Unlock()

			if currentMount != m.ID {
				log.Printf("[%s] 🐎 Montaria ativa encontrada: '%s' (ID %d). Montando para bônus de velocidade...", b.ID(), m.Name, m.ID)
				b.addLog(fmt.Sprintf("Montando em '%s' (+Speed)", m.Name))
				_ = b.socket.SendJSON(map[string]any{
					"t":  "mount",
					"id": m.ID,
				})
			}
			break
		}
	}
}

func (b *Bot) checkAndHandleSkills() {
	_ = b.socket.SendJSON(map[string]any{"t": "arvore"})
}

func (b *Bot) handleArvoreReceived(data []byte) {
	var p struct {
		View struct {
			Pontos struct {
				Livres int `json:"livres"`
				Total  int `json:"total"`
				Gastos int `json:"gastos"`
			} `json:"pontos"`
			Nos []struct {
				ID    string `json:"id"`
				Via   string `json:"via"`
				Graus int    `json:"graus"`
				Max   int    `json:"max"`
			} `json:"nos"`
			Plano map[string]int `json:"plano"`
		} `json:"view"`
	}
	if err := json.Unmarshal(data, &p); err != nil || len(p.View.Nos) == 0 {
		return
	}

	livres := p.View.Pontos.Livres
	if livres <= 0 {
		return
	}

	log.Printf("[%s] 🌳 Árvore de Habilidades: %d pontos livres encontrados! Distribuindo...", b.ID(), livres)

	// Cria cópia do plano atual
	plano := make(map[string]int)
	for k, v := range p.View.Plano {
		plano[k] = v
	}

	distribuidos := 0
	// 1. Prioriza nós que já têm pontos alocados (> 0) e ainda não atingiram o máximo
	for _, no := range p.View.Nos {
		if livres <= 0 {
			break
		}
		atual := plano[no.ID]
		maximo := no.Max
		if maximo <= 0 {
			maximo = 5
		}
		if atual > 0 && atual < maximo {
			falta := maximo - atual
			alocar := falta
			if alocar > livres {
				alocar = livres
			}
			plano[no.ID] = atual + alocar
			livres -= alocar
			distribuidos += alocar
		}
	}
	// 2. Se ainda restarem pontos, aloca em nós raiz seguros (graus == 0)
	if livres > 0 {
		for _, no := range p.View.Nos {
			if livres <= 0 {
				break
			}
			atual := plano[no.ID]
			maximo := no.Max
			if maximo <= 0 {
				maximo = 5
			}
			if no.Graus == 0 && atual < maximo {
				falta := maximo - atual
				alocar := falta
				if alocar > livres {
					alocar = livres
				}
				plano[no.ID] = atual + alocar
				livres -= alocar
				distribuidos += alocar
			}
		}
	}

	if distribuidos > 0 {
		log.Printf("[%s] 🌳 Enviando plano da Árvore de Habilidades com %d pontos alocados!", b.ID(), distribuidos)
		b.addLog(fmt.Sprintf("Árvore de Habilidades: +%d pontos distribuídos", distribuidos))
		_ = b.socket.SendJSON(map[string]any{
			"t":      "arvore",
			"action": "aplicar",
			"plano":  plano,
		})
	}
}

func (b *Bot) checkAndHandleCharms() {
	_ = b.socket.SendJSON(map[string]any{"t": "charms"})
}

func (b *Bot) handleCharmsReceived(data []byte) {
	var p struct {
		View struct {
			Points int `json:"points"`
			Charms []struct {
				ID       string `json:"id"`
				Name     string `json:"name"`
				Cost     int    `json:"cost"`
				Unlocked bool   `json:"unlocked"`
				Assigned string `json:"assigned"`
			} `json:"charms"`
		} `json:"view"`
	}
	if err := json.Unmarshal(data, &p); err != nil || len(p.View.Charms) == 0 {
		return
	}

	points := p.View.Points
	for _, c := range p.View.Charms {
		if !c.Unlocked && points >= c.Cost && c.Cost > 0 {
			log.Printf("[%s] 🔮 Desbloqueando Charm '%s' (Custo: %d pontos)...", b.ID(), c.Name, c.Cost)
			b.addLog(fmt.Sprintf("Charm '%s' desbloqueado!", c.Name))
			_ = b.socket.SendJSON(map[string]any{
				"t":      "charms",
				"action": "upgrade",
				"id":     c.ID,
			})
			points -= c.Cost
		}
	}

	b.mu.RLock()
	hunt := b.hunt
	b.mu.RUnlock()

	targetMonster := ""
	if hunt != nil && len(hunt.Monsters) > 0 {
		if m, ok := hunt.Monsters[0].(map[string]any); ok {
			if id, ok := m["id"].(string); ok {
				targetMonster = id
			}
		}
	}

	for _, c := range p.View.Charms {
		if c.Unlocked && c.Assigned == "" && targetMonster != "" {
			log.Printf("[%s] 🔮 Vinculando Charm '%s' ao monstro '%s'...", b.ID(), c.Name, targetMonster)
			b.addLog(fmt.Sprintf("Charm '%s' vinculado a '%s'", c.Name, targetMonster))
			_ = b.socket.SendJSON(map[string]any{
				"t":       "charms",
				"action":  "assign",
				"id":      c.ID,
				"monster": targetMonster,
			})
		}
	}
}

func (b *Bot) checkAndHandleCoinMarket() {
	_ = b.socket.SendJSON(map[string]any{
		"t":      "coinMarket",
		"pagina": 1,
	})
}

func (b *Bot) handleCoinMarketReceived(data []byte) {
	var p struct {
		Orders []struct {
			ID     string `json:"id"`
			Kind   string `json:"kind"`
			Amount int    `json:"amount"`
			Price  int64  `json:"price"`
		} `json:"orders"`
	}
	if err := json.Unmarshal(data, &p); err != nil || len(p.Orders) == 0 {
		return
	}

	var lowestSell int64 = 0
	for _, o := range p.Orders {
		if o.Kind == "sell" && (lowestSell == 0 || o.Price < lowestSell) {
			lowestSell = o.Price
		}
	}

	if lowestSell > 0 {
		log.Printf("[%s] 💰 Cotação Lux Coin: 1 Coin = %d ouro", b.ID(), lowestSell)
	}
}

func (b *Bot) checkAndHandlePrey() {
	b.mu.RLock()
	char := b.character
	b.mu.RUnlock()

	if char == nil || len(char.Prey) == 0 {
		return
	}

	nowMs := time.Now().UnixMilli()

	for _, slot := range char.Prey {
		if slot.State == "locked" {
			continue
		}

		key := ""
		if slot.Key != nil {
			key = *slot.Key
		}

		bonus := ""
		if slot.Bonus != nil {
			bonus = *slot.Bonus
		}

		// Se a presa expirou ou está sem criatura selecionada
		if key == "" || slot.Left <= 0 {
			if len(slot.Options) > 0 {
				chosen := slot.Options[0]

				// Tenta priorizar uma criatura da área atual
				b.mu.RLock()
				currentHuntID := ""
				if b.hunt != nil {
					currentHuntID = b.hunt.HuntID
				}
				b.mu.RUnlock()

				huntCreatures := make(map[string]bool)
				if b.catalog != nil && currentHuntID != "" {
					for _, h := range b.catalog.Hunts {
						if h.ID == currentHuntID {
							for _, c := range h.Creatures {
								if cm, ok := c.(map[string]any); ok {
									if k, ok := cm["key"].(string); ok {
										huntCreatures[k] = true
									}
								}
							}
							break
						}
					}
				}

				matched := false
				for _, opt := range slot.Options {
					if huntCreatures[opt] {
						chosen = opt
						matched = true
						break
					}
				}

				if !matched && slot.FreeRerollAte <= nowMs {
					log.Printf("[%s] 🎯 AutoPrey: Nenhuma criatura da área atual no Slot %d. Rerolando lista...", b.ID(), slot.Index+1)
					_ = b.socket.SendJSON(map[string]any{
						"t":      "prey",
						"action": "rerollList",
						"slot":   slot.Index,
					})
				} else {
					log.Printf("[%s] 🎯 AutoPrey: escolhendo presa '%s' no Slot %d (Bônus: %s +%d%%)", b.ID(), chosen, slot.Index+1, bonus, slot.Percent)
					b.addLog(fmt.Sprintf("Prey Slot %d: presa '%s' selecionada (+%d%%)", slot.Index+1, chosen, slot.Percent))
					_ = b.socket.SendJSON(map[string]any{
						"t":      "prey",
						"action": "choose",
						"slot":   slot.Index,
						"key":    chosen,
					})
				}
			} else if slot.FreeRerollAte <= nowMs {
				log.Printf("[%s] 🎯 AutoPrey: reroll grátis de lista no Slot %d", b.ID(), slot.Index+1)
				_ = b.socket.SendJSON(map[string]any{
					"t":      "prey",
					"action": "rerollList",
					"slot":   slot.Index,
				})
			}
		}
	}
}

func (b *Bot) startSafeHunt() {
	b.mu.Lock()
	char := b.character
	cfg := b.config.Settings
	b.mu.Unlock()

	if char == nil {
		return
	}

	targetHuntID := b.resolveBestHunt(char.Level)
	mode := cfg.HuntMode
	if mode == "" || mode == "online" {
		mode = "cycle" // "cycle" é a Caça Automática oficial do Lux Idle
	}
	strat := cfg.Strategy
	if strat == "" {
		strat = "nearest"
	}

	token := generateActionToken(fmt.Sprintf("startHunt:%s:%s", targetHuntID, mode))

	log.Printf("[%s] 🏹 Iniciando Caça Automática (modo ciclo) em '%s' (alvo: %s)...", b.ID(), targetHuntID, strat)
	b.addLog(fmt.Sprintf("Iniciando caçada em '%s'", targetHuntID))

	_ = b.socket.SendJSON(map[string]any{
		"t":        "startHunt",
		"huntId":   targetHuntID,
		"strategy": strat,
		"mode":     mode,
		"ids":      []string{targetHuntID},
		"acao":     token,
		"limites":  map[string]any{},
	})

	// Configurações de distância e lure
	_ = b.socket.SendJSON(map[string]any{"t": "strategy", "value": strat})
	_ = b.socket.SendJSON(map[string]any{"t": "distance", "value": cfg.Distance})
	_ = b.socket.SendJSON(map[string]any{"t": "lure", "value": cfg.Lure})

	if cfg.AutoAssist {
		_ = b.socket.SendJSON(map[string]any{"t": "huntAssist", "tipo": "ataque", "on": true})
		_ = b.socket.SendJSON(map[string]any{"t": "huntAssist", "tipo": "barra", "on": true})
	}
}

func (b *Bot) checkAndTriggerBoss() {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.catalog == nil || len(b.catalog.Bosses) == 0 {
		return
	}

	charLevel := 1
	if b.character != nil {
		charLevel = b.character.Level
	}

	var readyBosses []string
	for _, boss := range b.catalog.Bosses {
		if boss.Level <= charLevel {
			cooldown := int64(0)
			if b.character != nil && b.character.BossCooldowns != nil {
				cooldown = b.character.BossCooldowns[boss.ID]
			}
			if cooldown <= 0 {
				readyBosses = append(readyBosses, boss.ID)
			}
		}
	}

	if len(readyBosses) > 0 {
		log.Printf("[%s] 👑 Iniciando rotação com %d boss(es) disponível(is)...", b.ID(), len(readyBosses))
		_ = b.socket.SendJSON(map[string]any{
			"t":      "autoBoss",
			"action": "start",
			"ids":    readyBosses,
		})
	}
}

// -------------------------------------------------------------
// Catálogo Dinâmico e Seleção de Hunt Inteligente
// -------------------------------------------------------------

func (b *Bot) mergeCatalog(c *CatalogData) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mergeCatalogLocked(c)
}

func (b *Bot) mergeCatalogLocked(c *CatalogData) {
	if len(c.Hunts) > 0 {
		b.catalog.Hunts = c.Hunts
	}
	if len(c.Bosses) > 0 {
		b.catalog.Bosses = c.Bosses
	}
	if len(c.Training) > 0 {
		b.catalog.Training = c.Training
	}
}

func (b *Bot) resolveBestHunt(level int) string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// 1. Se o usuário fixou um mapa específico no settings
	target := b.config.Settings.HuntTarget
	if target != "" && target != "auto" {
		return target
	}

	// 2. Sincronização Inteligente de Party:
	// - Se for líder: limita a escolha ao menor nível da party ativa para ninguém ficar de fora
	// - Se for membro: caça no mesmo mapa do líder se nosso nível permitir
	effectiveLevel := level
	isLeader := (b.config.Settings.PartyLeader != "" && b.character != nil && strings.EqualFold(b.character.Name, b.config.Settings.PartyLeader)) ||
		(b.character != nil && b.character.Party != nil && b.character.Party.SouLider)

	if b.config.Settings.AutoParty && b.manager != nil {
		statuses := b.manager.GetAllStatuses()
		if isLeader {
			for _, st := range statuses {
				if st.ID == b.ID() || st.Status == "training (dummies)" {
					continue
				}
				// Se o líder tem lista explícita de membros da party, só considera eles
				if len(b.config.Settings.PartyMembers) > 0 {
					isMember := false
					for _, m := range b.config.Settings.PartyMembers {
						if strings.EqualFold(st.Name, m) {
							isMember = true
							break
						}
					}
					if !isMember {
						continue
					}
				}
				if st.Level > 0 && st.Level < effectiveLevel {
					effectiveLevel = st.Level
				}
			}
		} else if b.config.Settings.PartyLeader != "" {
			for _, st := range statuses {
				if strings.EqualFold(st.Name, b.config.Settings.PartyLeader) && st.CurrentArea != "" && st.Status == "hunting" {
					if b.catalog != nil && len(b.catalog.Hunts) > 0 {
						for _, h := range b.catalog.Hunts {
							if h.ID == st.CurrentArea && level >= h.Level {
								return st.CurrentArea
							}
						}
					} else {
						return st.CurrentArea
					}
				}
			}
		}
	}

	// 3. Se temos catálogo recebido do servidor, filtra os mapas onde level <= effectiveLevel
	if b.catalog != nil && len(b.catalog.Hunts) > 0 {
		eligible := make([]CatalogHunt, 0)
		for _, h := range b.catalog.Hunts {
			if h.Level <= effectiveLevel {
				eligible = append(eligible, h)
			}
		}

		if len(eligible) > 0 {
			// Ordena por level decrescente (o mais desafiador e lucrativo compatível)
			sort.Slice(eligible, func(i, j int) bool {
				return eligible[i].Level > eligible[j].Level
			})
			return eligible[0].ID
		}
		return b.catalog.Hunts[0].ID
	}

	// 4. Fallback inicial seguro com IDs canônicos do Lux Idle
	if effectiveLevel < 12 {
		return "troll-cave"
	} else if effectiveLevel < 20 {
		return "vip-rotworm"
	} else if effectiveLevel < 40 {
		return "vip-cyclops"
	} else if effectiveLevel < 60 {
		return "mistrock-cyclops"
	} else if effectiveLevel < 120 {
		return "dark-pyramid"
	}
	return "putrid-mummies"
}

// -------------------------------------------------------------
// Ações Manuais via Web Dashboard / API
// -------------------------------------------------------------

func (b *Bot) GetStatus() engine.AccountStatus {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.status
}

func (b *Bot) ExecuteAction(action string, payload map[string]any) (any, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.socket == nil || !b.socket.IsConnected() {
		return nil, fmt.Errorf("bot não conectado ao servidor")
	}

	switch action {
	case "startHunt":
		huntID, _ := payload["huntId"].(string)
		if huntID == "" {
			huntID = b.resolveBestHunt(b.status.Level)
		}
		token := generateActionToken("startHunt:" + huntID + ":cycle")
		err := b.socket.SendJSON(map[string]any{
			"t":        "startHunt",
			"huntId":   huntID,
			"strategy": "nearest",
			"mode":     "cycle",
			"ids":      []string{huntID},
			"acao":     token,
			"limites":  map[string]any{},
		})
		b.addLogLocked("Caçada iniciada manualmente.")
		return fmt.Sprintf("Hunt %s iniciada", huntID), err

	case "stopHunt":
		err := b.socket.SendJSON(map[string]any{"t": "stopHunt"})
		b.addLogLocked("Caçada encerrada manualmente.")
		return "Caçada encerrada", err

	case "toggleAutoHunt":
		b.config.Settings.AutoHunt = !b.config.Settings.AutoHunt
		estado := "ativado"
		if !b.config.Settings.AutoHunt {
			estado = "desativado"
		}
		b.addLogLocked(fmt.Sprintf("Auto-hunt %s manualmente.", estado))
		return map[string]any{"autoHunt": b.config.Settings.AutoHunt}, nil

	case "sell":
		_ = b.socket.SendJSON(map[string]any{"t": "venderSacolas"})
		_ = b.socket.SendJSON(map[string]any{"t": "venderMochila"})
		_ = b.socket.SendJSON(map[string]any{"t": "clearPouch"})
		b.addLogLocked("Venda e limpeza de inventário acionada.")
		return "Loot vendido", nil

	case "bless":
		err := b.socket.SendJSON(map[string]any{"t": "bless", "all": true})
		b.addLogLocked("Bênçãos compradas.")
		return "Bênçãos solicitadas", err

	case "reward":
		err := b.socket.SendJSON(map[string]any{"t": "reward", "action": "takeAll"})
		b.addLogLocked("Recompensas diárias coletadas.")
		return "Recompensas diárias coletadas", err

	case "promote":
		err := b.socket.SendJSON(map[string]any{"t": "promote"})
		b.addLogLocked("Promoção de vocação solicitada.")
		return "Promoção enviada", err

	case "train":
		err := b.socket.SendJSON(map[string]any{"t": "training", "action": "start", "mode": "online"})
		b.addLogLocked("Treino online acionado.")
		return "Treino iniciado", err

	default:
		return nil, fmt.Errorf("ação desconhecida: %s", action)
	}
}

func (b *Bot) SupplyTicket(ticket, wsURL string) error {
	return nil
}

func (b *Bot) SupplyTurnstileToken(token string) error {
	return nil
}

// -------------------------------------------------------------
// Helpers Internos de Estado e Tokens
// -------------------------------------------------------------

func (b *Bot) updateStatus(fn func(s *engine.AccountStatus)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	fn(&b.status)
	b.status.UpdatedAt = time.Now().UnixMilli()
}

func (b *Bot) addLog(msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.addLogLocked(msg)
}

func (b *Bot) addLogLocked(msg string) {
	b.status.Logs = append(b.status.Logs, fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), msg))
	if len(b.status.Logs) > 15 {
		b.status.Logs = b.status.Logs[1:]
	}
}

// generateActionToken replica a função MN() do Lux Idle: random base36 + timestamp base36
func generateActionToken(key string) string {
	bytes := make([]byte, 4)
	_, _ = rand.Read(bytes)
	randStr := hex.EncodeToString(bytes)

	nowStr := strconv.FormatInt(time.Now().UnixMilli(), 36)
	if len(nowStr) > 4 {
		nowStr = nowStr[len(nowStr)-4:]
	}
	return randStr + nowStr
}

func (b *Bot) resolveTrainingSkill(vocation string) string {
	voc := strings.ToLower(vocation)
	switch {
	case strings.Contains(voc, "knight"):
		return "sword"
	case strings.Contains(voc, "paladin"):
		return "distance"
	case strings.Contains(voc, "sorcerer"), strings.Contains(voc, "druid"):
		return "magic"
	case strings.Contains(voc, "monk"):
		return "fist"
	default:
		return "magic"
	}
}

func (b *Bot) checkAndHandleEquipment() {
	b.mu.RLock()
	char := b.character
	b.mu.RUnlock()

	if char == nil || len(char.Inventory) == 0 {
		return
	}

	for _, item := range char.Inventory {
		slot := item.Slot
		if slot == "" || slot == "ammo" {
			continue
		}

		current, hasEquip := char.Equipment[slot]
		isUpgrade := false

		// 1. Se o slot estiver vazio (ex: botas, anel, amuleto, escudo), equipa imediatamente
		if !hasEquip || current.GetID() == "" || current.GetID() == "0" {
			isUpgrade = true
		} else {
			// 2. Se tiver item equipado, verifica se o item do inventário é um tier superior
			curName := strings.ToLower(current.Name)
			newItemName := strings.ToLower(item.Name)
			if (strings.Contains(curName, "leather") || strings.Contains(curName, "wooden") || strings.Contains(curName, "studded") || strings.Contains(curName, "club")) &&
				(strings.Contains(newItemName, "plate") || strings.Contains(newItemName, "steel") || strings.Contains(newItemName, "iron") || strings.Contains(newItemName, "sword") || strings.Contains(newItemName, "axe") || strings.Contains(newItemName, "wand") || strings.Contains(newItemName, "rod")) {
				isUpgrade = true
			}
		}

		if isUpgrade {
			log.Printf("[%s] 🛡️ AutoEquip: Equipando upgrade '%s' no slot '%s'...", b.ID(), item.Name, slot)
			b.addLog(fmt.Sprintf("AutoEquip: '%s' (%s)", item.Name, slot))
			_ = b.socket.SendJSON(map[string]any{
				"t":    "equip",
				"id":   item.ID,
				"slot": slot,
			})
			time.Sleep(300 * time.Millisecond)
		}
	}
}

func (b *Bot) checkAndHandleGoldTransfer() {
	b.mu.RLock()
	char := b.character
	cfg := b.config.Settings
	b.mu.RUnlock()

	if char == nil {
		return
	}

	mainAcc := cfg.MainAccount
	if mainAcc == "" {
		mainAcc = "kbelludoo"
	}

	if strings.EqualFold(char.Name, mainAcc) {
		return
	}

	reserve := int64(1000)
	if char.Gold > reserve+500 {
		transferAmount := char.Gold - reserve
		log.Printf("[%s] 💰 AutoBank: Centralizando %d de ouro para a conta principal '%s'...", b.ID(), transferAmount, mainAcc)
		b.addLog(fmt.Sprintf("Enviou %d de ouro para %s", transferAmount, mainAcc))
		_ = b.socket.SendJSON(map[string]any{
			"t":      "bank",
			"action": "transfer",
			"name":   mainAcc,
			"amount": transferAmount,
		})
	}
}

func (b *Bot) checkAndHandleGemBag() {
	b.mu.RLock()
	char := b.character
	b.mu.RUnlock()

	if char == nil {
		return
	}

	_ = b.socket.SendJSON(map[string]any{"t": "gemBag"})
	_ = b.socket.SendJSON(map[string]any{"t": "gemas"})

	for _, item := range char.Inventory {
		isGem := item.Tipo == "gema" || strings.Contains(strings.ToLower(item.Name), "gema") ||
			strings.Contains(strings.ToLower(item.Name), "gem")
		if isGem && item.GetID() != "" {
			_ = b.socket.SendJSON(map[string]any{
				"t":      "gemBag",
				"acao":   "guardar",
				"itemId": item.ID,
				"count":  item.Count,
				"origem": "bag",
			})
		}
	}
}

func (b *Bot) checkAndApplyActionPresets() {
	b.mu.RLock()
	char := b.character
	b.mu.RUnlock()

	if char == nil || len(char.ActionPresets) == 0 {
		return
	}

	if char.ActionPresetAtual == "" {
		presetName := char.ActionPresets[0].Name
		log.Printf("[%s] 🔮 AutoSpell: Aplicando preset de magias e ações: '%s'...", b.ID(), presetName)
		b.addLog(fmt.Sprintf("Preset de magias ativado: '%s'", presetName))
		_ = b.socket.SendJSON(map[string]any{
			"t":      "actionPreset",
			"action": "apply",
			"name":   presetName,
		})
	}
}
