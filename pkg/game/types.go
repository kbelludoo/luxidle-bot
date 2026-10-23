package game

import "fmt"

// AccountConfig modelo de configuração do bot lido do accounts.json
type AccountConfig struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Enabled     bool            `json:"enabled"`
	BindIP      string          `json:"bindIP,omitempty"` // IP secundário na VPS para multi-contas isoladas
	Credentials GameCredentials `json:"credentials"`
	Settings    GameSettings    `json:"settings"`
}

type GameCredentials struct {
	Email         string `json:"email"`
	Password      string `json:"password"`
	CharacterName string `json:"characterName,omitempty"` // Se vazio, seleciona o primeiro personagem da conta
	Vocation      string `json:"vocation,omitempty"`
	SessionToken  string `json:"token,omitempty"`
}

type GameSettings struct {
	HuntTarget     string   `json:"huntTarget"`               // ID da hunt ou "auto" para melhor mapa por level
	HuntMode       string   `json:"huntMode,omitempty"`       // "online" ou "auto" (default: "online")
	Strategy       string   `json:"strategy,omitempty"`       // "nearest", "lowest", "highest"
	Distance       int      `json:"distance"`                 // 0 (melee) a 6 (ranged)
	Lure           int      `json:"lure"`                     // 0 a 10
	AutoHunt       bool     `json:"autoHunt"`                 // Caçar automaticamente
	AutoSell       bool     `json:"autoSell"`                 // Vender loot da mochila/sacolas ao encher
	AutoBless      bool     `json:"autoBless"`                // Comprar todas as bênçãos (bless)
	AutoDaily      bool     `json:"autoDaily"`                // Resgatar recompensas diárias
	AutoTasks      bool     `json:"autoTasks"`                // Aceitar e entregar tasks de monstros
	AutoBoss       bool     `json:"autoBoss"`                 // Fazer rotação diária de bosses disponíveis
	AutoTraining   bool     `json:"autoTraining"`             // Treinar boneco online quando ocioso
	AutoPromote    bool     `json:"autoPromote"`              // Promover vocação no level 20+
	AutoParty      bool     `json:"autoParty"`                // Convidar e sincronizar caçada em grupo (party)
	PartyLeader    string   `json:"partyLeader,omitempty"`    // Nome do personagem líder da party (ex: klord)
	PartyMembers   []string `json:"partyMembers,omitempty"`   // Nomes dos membros da party (ex: lrN, secondpally, mojo)
	AutoAssist     bool     `json:"autoAssist"`               // Manter auto-ataque e auto-barra ativados
	AutoPrey       bool     `json:"autoPrey"`                 // Selecionar e renovar bônus de Prey (presas) automaticamente
	AutoOpenBoxes  bool     `json:"autoOpenBoxes"`            // Abrir caixas de boss, baús e bolsas automaticamente
	AutoQuiver     bool     `json:"autoQuiver"`               // Guardar munição na aljava automaticamente (Paladino)
	AutoMount      bool     `json:"autoMount"`                // Equipar montaria automaticamente (+Speed)
	AutoSkills     bool     `json:"autoSkills"`               // Distribuir pontos na árvore de habilidades
	AutoCharms     bool     `json:"autoCharms"`               // Desbloquear e equipar charms do bestiário
	AutoCoinMarket bool     `json:"autoCoinMarket"`           // Monitorar cotação do mercado de Lux Coins
	MinHPPercent   int      `json:"minHpPercent,omitempty"`   // % de vida mínima para recuar com segurança (default 30)
}

// -------------------------------------------------------------
// Modelos de Estado do Jogo (Recebidos do Servidor)
// -------------------------------------------------------------

type CharacterInfo struct {
	Name     string `json:"name"`
	Level    int    `json:"level"`
	Vocation string `json:"vocation"`
	Sex      string `json:"sex"`
}

type CharacterAccount struct {
	Email      string          `json:"email"`
	Characters []CharacterInfo `json:"characters"`
	Limite     int             `json:"limite"`
}

type CharacterState struct {
	Name          string               `json:"name"`
	Level         int                  `json:"level"`
	Vocation      string               `json:"vocation"`
	Sex           string               `json:"sex"`
	HP            int                  `json:"hp"`
	Mana          int                  `json:"mana"`
	Gold          int64                `json:"gold"`
	Coins         int64                `json:"coins,omitempty"`
	Weight        float64              `json:"weight"`
	Derived       *DerivedStats        `json:"derived,omitempty"`
	Progress      *ProgressStats       `json:"progress,omitempty"`
	PouchValue    *PouchValue          `json:"pouchValue,omitempty"`
	Party         *PartyState          `json:"party,omitempty"`
	AutoBoss      *AutoBossState       `json:"autoBoss,omitempty"`
	BossCooldowns map[string]int64     `json:"bossCooldowns,omitempty"`
	BossTasks     []BossTask           `json:"bossTasks,omitempty"`
	Blessings     any                  `json:"blessings,omitempty"`
	Stamina       any                  `json:"stamina,omitempty"`
	MaxStamina    any                  `json:"maxStamina,omitempty"`
	TreinoStamina any                  `json:"treinoStamina,omitempty"`
	Prey          []PreySlot           `json:"prey,omitempty"`
	Wildcards     int                  `json:"wildcards,omitempty"`
	ArvorePontos  *ArvorePontos        `json:"arvorePontos,omitempty"`
	CharmPoints   int                  `json:"charmPoints,omitempty"`
	Mount         int                  `json:"mount,omitempty"`
	Mounts        any                  `json:"mounts,omitempty"`
	Inventory     []ItemState          `json:"inventory,omitempty"`
	Equipment     map[string]ItemState `json:"equipment,omitempty"`
}

type ItemState struct {
	ID     any    `json:"id"`
	Name   string `json:"name,omitempty"`
	Count  int    `json:"count,omitempty"`
	Slot   string `json:"slot,omitempty"`
	Usavel bool   `json:"usavel,omitempty"`
	Ammo   bool   `json:"ammo,omitempty"`
	Tipo   string `json:"tipo,omitempty"`
}

func (i ItemState) GetID() string {
	if i.ID == nil {
		return ""
	}
	return fmt.Sprintf("%v", i.ID)
}

type ArvorePontos struct {
	Livres int `json:"livres"`
	Gastos int `json:"gastos"`
	Total  int `json:"total"`
}

type MountsState struct {
	Mounts  []MountInfo `json:"mounts"`
	Current int         `json:"current"`
}

type MountInfo struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Owned bool   `json:"owned"`
}

type PreySlot struct {
	Index         int      `json:"index"`
	State         string   `json:"state"`
	Key           *string  `json:"key"`
	Bonus         *string  `json:"bonus"`
	Percent       int      `json:"percent"`
	Rarity        int      `json:"rarity"`
	Left          int64    `json:"left"`
	Total         int64    `json:"total"`
	FreeRerollAte int64    `json:"freeRerollAte"`
	Options       []string `json:"options"`
	Locked        bool     `json:"locked"`
	AutoReroll    bool     `json:"autoReroll"`
}

type DerivedStats struct {
	MaxHP    int     `json:"maxHp"`
	MaxMana  int     `json:"maxMana"`
	Capacity float64 `json:"capacity"`
	Speed    float64 `json:"speed"`
}

type ProgressStats struct {
	Exp    int64 `json:"exp"`
	ToNext int64 `json:"toNext"`
}

type PouchValue struct {
	Gold  int64 `json:"gold"`
	Itens int   `json:"itens"`
}

type PartyState struct {
	Ativa    bool          `json:"ativa,omitempty"`
	Bonus    float64       `json:"bonus,omitempty"`
	Vocacoes int           `json:"vocacoes,omitempty"`
	Lider    string        `json:"lider,omitempty"`
	SouLider bool          `json:"souLider,omitempty"`
	Membros  []PartyMember `json:"membros,omitempty"`
}

type PartyMember struct {
	Name     string `json:"name"`
	Level    int    `json:"level,omitempty"`
	Vocation string `json:"vocation,omitempty"`
	Online   bool   `json:"online,omitempty"`
	Lider    bool   `json:"lider,omitempty"`
	Estado   string `json:"estado,omitempty"`
}

func (p *PartyState) GetLeader() string {
	if p == nil {
		return ""
	}
	if p.Lider != "" {
		return p.Lider
	}
	for _, m := range p.Membros {
		if m.Lider {
			return m.Name
		}
	}
	return ""
}

type AutoBossState struct {
	Bosses       []string `json:"bosses"`
	ConvidadoPor string   `json:"convidadoPor,omitempty"`
}

type BossTask struct {
	Boss  string `json:"boss"`
	Feito bool   `json:"feito"`
	Matou bool   `json:"matou"`
}

type HuntState struct {
	HuntID      string       `json:"huntId"`
	Manual      bool         `json:"manual"`
	Assistencia bool         `json:"assistencia"`
	AutoBarra   bool         `json:"autoBarra"`
	Monsters    []any        `json:"monsters,omitempty"`
	Session     *HuntSession `json:"session,omitempty"`
}

type HuntSession struct {
	Kills       int     `json:"kills"`
	Exp         int64   `json:"exp"`
	Gold        int64   `json:"gold"`
	LootValue   int64   `json:"lootValue"`
	Supplies    int64   `json:"supplies"`
	ExpDoGrupo  int64   `json:"expDoGrupo"`
	Elites      int     `json:"elites"`
	ExpPerHour  float64 `json:"expPerHour,omitempty"`
	DurationSec int     `json:"durationSec,omitempty"`
}

type CatalogData struct {
	Hunts    []CatalogHunt `json:"hunts,omitempty"`
	Bosses   []CatalogHunt `json:"bosses,omitempty"`
	Training []CatalogHunt `json:"training,omitempty"`
}

type CatalogHunt struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Level     int    `json:"level"`
	Creatures []any  `json:"creatures,omitempty"`
}

// -------------------------------------------------------------
// Pacotes Inbound / Outbound do Protocolo Lux Idle
// -------------------------------------------------------------

type BasePacket struct {
	T string `json:"t"`
}

type InboundPacket struct {
	T           string            `json:"t"`
	Catalog     *CatalogData      `json:"catalog,omitempty"`
	Account     *CharacterAccount `json:"account,omitempty"`
	Token       string            `json:"token,omitempty"`
	Message     string            `json:"message,omitempty"`
	Code        string            `json:"code,omitempty"`
	Character   *CharacterState   `json:"character,omitempty"`
	Hunt        *HuntState        `json:"hunt,omitempty"`
	CharDelta   bool              `json:"charDelta,omitempty"`
	HuntDelta   bool              `json:"huntDelta,omitempty"`
	SessaoDelta bool              `json:"sessaoDelta,omitempty"`
	Tasks       any               `json:"tasks,omitempty"`
	Lista       any               `json:"lista,omitempty"`
	At          int64             `json:"at,omitempty"`
}
