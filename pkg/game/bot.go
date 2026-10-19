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
	lastStateAt  time.Time

	// Canais de sinalização
	accountReady chan struct{}
	ticketChan   chan string
	discChan     chan error
}

func NewBot(cfg AccountConfig) *Bot {
	st := engine.NewStatus(cfg.ID, cfg.Name)
	st.CurrentArea = cfg.Settings.HuntTarget
	if st.CurrentArea == "" {
		st.CurrentArea = "auto"
	}

	b := &Bot{
		config:       cfg,
		status:       st,
		catalog:      &CatalogData{},
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
		if err := json.Unmarshal(data, &p); err == nil {
			b.handleWelcomeReceived(p)
		}

	case "state":
		var p InboundPacket
		if err := json.Unmarshal(data, &p); err == nil {
			b.handleStateReceived(p)
		}

	case "tasksDeBicho":
		b.handleTasksReceived(data)

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
		log.Printf("[%s] Aviso do jogo: %s", b.ID(), p.Message)
		b.addLog(fmt.Sprintf("Aviso: %s", p.Message))
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

func (b *Bot) handleAccountReceived(p InboundPacket) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if p.Token != "" {
		b.sessionToken = p.Token
		b.config.Credentials.SessionToken = p.Token
	}

	if p.Account == nil || len(p.Account.Characters) == 0 {
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
			if p.Character.Derived != nil {
				b.character.Derived = p.Character.Derived
			}
			if p.Character.Progress != nil {
				b.character.Progress = p.Character.Progress
			}
			if p.Character.Party != nil {
				b.character.Party = p.Character.Party
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
		}

		b.status.Level = b.character.Level
		b.status.Vocation = b.character.Vocation
		b.status.HP = b.character.HP
		b.status.Mana = b.character.Mana
		b.status.Gold = b.character.Gold

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
		if b.status.Status == "hunting" {
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

	// Se houver lista de tasks, busca alguma concluída para resgatar
	if lista, ok := raw["tasks"].([]any); ok {
		for _, item := range lista {
			if m, ok := item.(map[string]any); ok {
				key, _ := m["key"].(string)
				feito, _ := m["feito"].(bool)
				if key != "" && feito {
					log.Printf("[%s] Resgatando recompensa de task: %s", b.ID(), key)
					_ = b.socket.SendJSON(map[string]any{
						"t":      "tasksDeBicho",
						"action": "resgatar",
						"key":    key,
					})
				}
			}
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

	if char == nil || char.Name == "" {
		return // Aguardando welcome/state do servidor
	}

	now := time.Now()

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
		log.Printf("[%s] Resgatando recompensas diárias...", b.ID())
		_ = b.socket.SendJSON(map[string]any{"t": "reward", "action": "takeAll"})
		_ = b.socket.SendJSON(map[string]any{"t": "diario"})
	}

	// 6. Limpeza e Venda de Inventário (AutoSell)
	if cfg.AutoSell && now.Sub(b.lastSellAt) > 2*time.Minute {
		b.lastSellAt = now
		_ = b.socket.SendJSON(map[string]any{"t": "venderSacolas"})
		_ = b.socket.SendJSON(map[string]any{"t": "venderMochila"})
		_ = b.socket.SendJSON(map[string]any{"t": "clearPouch"})
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

	// 9. Caçada Automática (AutoHunt) vs Treino Online nos Dummies (AutoTraining)
	if cfg.AutoHunt {
		if hunt == nil {
			// Não está caçando. Inicia caçada no melhor mapa!
			b.startSafeHunt()
		} else {
			// Já está caçando. Garante configurações de assistência e combate
			if cfg.AutoAssist {
				_ = b.socket.SendJSON(map[string]any{"t": "huntAssist", "tipo": "ataque", "on": true})
				_ = b.socket.SendJSON(map[string]any{"t": "huntAssist", "tipo": "barra", "on": true})
			}
		}
	} else if cfg.AutoTraining {
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

	// 11. AutoParty e Puxar Personagens (Multibox)
	if cfg.AutoParty && char.Party != nil && char.Party.SouLider {
		// Se for líder da party, puxa os outros personagens para a mesma caçada
		_ = b.socket.SendJSON(map[string]any{
			"t":    "meusChars",
			"acao": "puxar",
		})
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
				log.Printf("[%s] 🎯 AutoPrey: escolhendo presa '%s' no Slot %d (Bônus: %s +%d%%)", b.ID(), chosen, slot.Index+1, bonus, slot.Percent)
				b.addLog(fmt.Sprintf("Prey Slot %d: presa '%s' selecionada (+%d%%)", slot.Index+1, chosen, slot.Percent))
				_ = b.socket.SendJSON(map[string]any{
					"t":      "prey",
					"action": "choose",
					"slot":   slot.Index,
					"key":    chosen,
				})
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
	if mode == "" {
		mode = "online"
	}
	strat := cfg.Strategy
	if strat == "" {
		strat = "nearest"
	}

	token := generateActionToken(fmt.Sprintf("startHunt:%s:%s", targetHuntID, mode))

	log.Printf("[%s] 🏹 Iniciando caçada no mapa '%s' (modo: %s, alvo: %s)...", b.ID(), targetHuntID, mode, strat)
	b.addLog(fmt.Sprintf("Iniciando caçada em '%s'", targetHuntID))

	_ = b.socket.SendJSON(map[string]any{
		"t":        "startHunt",
		"huntId":   targetHuntID,
		"strategy": strat,
		"mode":     mode,
		"acao":     token,
		"limites":  map[string]any{},
	})

	// Configurações de distância e lure
	_ = b.socket.SendJSON(map[string]any{"t": "strategy", "value": strat})
	_ = b.socket.SendJSON(map[string]any{"t": "distance", "value": strconv.Itoa(cfg.Distance)})
	_ = b.socket.SendJSON(map[string]any{"t": "lure", "value": strconv.Itoa(cfg.Lure)})

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

	// 2. Se temos catálogo recebido do servidor, filtra os mapas onde level <= char.Level
	if b.catalog != nil && len(b.catalog.Hunts) > 0 {
		eligible := make([]CatalogHunt, 0)
		for _, h := range b.catalog.Hunts {
			if h.Level <= level {
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
		// Se nenhum tiver level <= char.Level, pega o primeiro da lista
		return b.catalog.Hunts[0].ID
	}

	// 3. Fallback inicial seguro com IDs canônicos do Lux Idle
	if level < 12 {
		return "vip-rotworm"
	} else if level < 30 {
		return "vip-cyclops"
	} else if level < 60 {
		return "hive-surface"
	} else if level < 120 {
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
		token := generateActionToken("startHunt:" + huntID + ":online")
		err := b.socket.SendJSON(map[string]any{
			"t":        "startHunt",
			"huntId":   huntID,
			"strategy": "nearest",
			"mode":     "online",
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
