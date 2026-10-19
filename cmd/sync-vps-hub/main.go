package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"sort"
	"sync"
	"time"

	"bot-template-go/pkg/ghsync"
	"bot-template-go/pkg/scan"
	"bot-template-go/pkg/stealth"
	"bot-template-go/pkg/vpsmetrics"
)

const fetchTimeout = 2500 * time.Millisecond

var reIDLogin = mustCompile(`(?i)\(ID:\s*\d+,\s*Login:\s*[^)]+\)`)

func mustCompile(expr string) *regexp.Regexp {
	return regexp.MustCompile(expr)
}

func fetchJSON(url string) map[string]any {
	client := &http.Client{Timeout: fetchTimeout}
	resp, err := client.Get(url)
	if err != nil {
		msg := err.Error()
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			msg = "Timeout de conexão"
		}
		return map[string]any{"online": false, "error": msg}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return map[string]any{"online": false, "error": err.Error()}
	}
	parsed, err := stealth.Decode(data)
	if err != nil {
		return map[string]any{"online": false, "error": "JSON inválido"}
	}
	out := map[string]any{"online": true}
	switch t := parsed.(type) {
	case map[string]any:
		for k, v := range t {
			out[k] = v
		}
	case []any:
		for i, v := range t {
			out[fmt.Sprintf("%d", i)] = v
		}
	}
	return out
}

func fetchAll() map[string]map[string]any {
	urls := map[string]string{
		"huntera":       "http://127.0.0.1:3000/api/status",
		"tibidle":       "http://127.0.0.1:3001/api/status",
		"sigma":         "http://127.0.0.1:3002/api/status",
		"leveling":      "http://127.0.0.1:3003/api/status",
		"pokeidle":      "http://127.0.0.1:3006/api/status",
		"pokeidleworld": "http://127.0.0.1:3007/api/status",
		"kickminer":     "http://127.0.0.1:3009/api/status",
		"baiak":         "http://127.0.0.1:8090/api/fleet",
		"twitchminer":   "http://127.0.0.1:3010/api/status",
		"lolidle":       "http://127.0.0.1:3011/api/status",
		"luxidle":       "http://127.0.0.1:3012/api/status",
	}
	results := make(map[string]map[string]any, len(urls))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for key, url := range urls {
		wg.Add(1)
		go func(key, url string) {
			defer wg.Done()
			val := fetchJSON(url)
			mu.Lock()
			results[key] = val
			mu.Unlock()
		}(key, url)
	}
	wg.Wait()
	return results
}

func isOnline(m map[string]any) bool {
	return m != nil && stealth.Truthy(m["online"])
}

func numGT(v any, threshold float64) bool {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		return err == nil && f > threshold
	case float64:
		return n > threshold
	}
	return false
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func nestedMap(m map[string]any, keys ...string) map[string]any {
	cur := m
	for _, k := range keys {
		next, ok := cur[k].(map[string]any)
		if !ok {
			return nil
		}
		cur = next
	}
	return cur
}

func accountsOf(bots map[string]any, bot string) []any {
	status := nestedMap(bots, bot, "status")
	if status == nil {
		return nil
	}
	accs, _ := status["accounts"].([]any)
	return accs
}

func eachAccount(bots map[string]any, bot string, fn func(i int, acc map[string]any)) {
	for i, a := range accountsOf(bots, bot) {
		if acc, ok := a.(map[string]any); ok {
			fn(i, acc)
		}
	}
}

func replaceLogNick(acc map[string]any, field, label string) {
	logs, ok := acc[field].([]any)
	if !ok {
		return
	}
	for i, l := range logs {
		if s, ok := l.(string); ok {
			logs[i] = stealth.NickRE.ReplaceAllLiteralString(s, label)
		}
	}
}

func sanitizeBotPayload(bots map[string]any) any {
	eachAccount(bots, "huntera", func(i int, acc map[string]any) {
		acc["charName"] = fmt.Sprintf("Personagem #%d", i+1)
		if _, ok := acc["id"].(string); ok {
			acc["id"] = fmt.Sprintf("acc%d", i+1)
		}
	})
	eachAccount(bots, "tibidle", func(i int, acc map[string]any) {
		acc["name"] = fmt.Sprintf("Conta #%d", i+1)
		if _, ok := acc["id"].(string); ok {
			acc["id"] = fmt.Sprintf("acc%d", i+1)
		}
		if logs, ok := acc["recentLogs"].([]any); ok {
			for j, l := range logs {
				if s, ok := l.(string); ok {
					logs[j] = scan.ReConnectedAs.ReplaceAllLiteralString(s, fmt.Sprintf("Conectado como Conta #%d", i+1))
				}
			}
		}
	})
	eachAccount(bots, "sigma", func(i int, acc map[string]any) {
		label := fmt.Sprintf("Treinador #%d", i+1)
		acc["trainerName"] = label
		acc["alias"] = label
		if _, ok := acc["id"].(string); ok {
			acc["id"] = fmt.Sprintf("acc%d", i+1)
		}
		delete(acc, "bindIp")
		delete(acc, "outboundIp")
	})
	eachAccount(bots, "leveling", func(i int, acc map[string]any) {
		label := fmt.Sprintf("Party #%d", i+1)
		acc["name"] = label
		if _, ok := acc["id"].(string); ok {
			acc["id"] = fmt.Sprintf("acc-%d", i+1)
		}
		replaceLogNick(acc, "logs", label)
	})
	eachAccount(bots, "pokeidle", func(i int, acc map[string]any) {
		label := fmt.Sprintf("Treinador #%d", i+1)
		acc["name"] = label
		if _, ok := acc["id"].(string); ok {
			acc["id"] = fmt.Sprintf("acc-%d", i+1)
		}
		replaceLogNick(acc, "logs", label)
	})
	eachAccount(bots, "pokeidleworld", func(i int, acc map[string]any) {
		label := fmt.Sprintf("Treinador #%d", i+1)
		acc["name"] = label
		if _, ok := acc["id"].(string); ok {
			acc["id"] = fmt.Sprintf("acc-%d", i+1)
		}
		if extra, ok := acc["extra"].(map[string]any); ok {
			if _, ok := extra["trainerName"].(string); ok {
				extra["trainerName"] = label
			}
		}
		replaceLogNick(acc, "logs", label)
	})
	eachAccount(bots, "lolidle", func(i int, acc map[string]any) {
		label := fmt.Sprintf("Campeão #%d", i+1)
		acc["name"] = label
		if _, ok := acc["id"].(string); ok {
			acc["id"] = fmt.Sprintf("acc-%d", i+1)
		}
		delete(acc, "bindIp")
		delete(acc, "outboundIp")
		replaceLogNick(acc, "logs", label)
	})
	if status := nestedMap(bots, "baiak", "status"); status != nil {
		if botMap, ok := status["bots"].(map[string]any); ok {
			for i, k := range sortedKeys(botMap) {
				b, ok := botMap[k].(map[string]any)
				if !ok {
					continue
				}
				b["name"] = fmt.Sprintf("Frota #%d", i+1)
				b["label"] = fmt.Sprintf("Conta #%d", i+1)
				if st, ok := b["status"].(map[string]any); ok {
					if chars, ok := st["characters"].([]any); ok {
						for ci, c := range chars {
							if cm, ok := c.(map[string]any); ok {
								cm["name"] = fmt.Sprintf("Personagem #%d", ci+1)
							}
						}
					}
				}
			}
		}
	}
	if status := nestedMap(bots, "kickminer", "status"); status != nil {
		if accMap, ok := status["accounts"].(map[string]any); ok {
			sanitized := make(map[string]any, len(accMap))
			for i, name := range sortedKeys(accMap) {
				item, ok := accMap[name].(map[string]any)
				if !ok {
					item = map[string]any{}
				}
				item["username"] = fmt.Sprintf("Minerador #%d", i+1)
				sanitized[fmt.Sprintf("miner_%d", i+1)] = item
			}
			status["accounts"] = sanitized
		}
	}
	if status := nestedMap(bots, "twitchminer", "status"); status != nil {
		if stMap, ok := status["statuses"].(map[string]any); ok {
			safe := make(map[string]any, len(stMap))
			for i, u := range sortedKeys(stMap) {
				acc, _ := stMap[u].(map[string]any)
				label := fmt.Sprintf("Minerador #%d", i+1)
				streamers := any(map[string]any{})
				if acc != nil {
					if s, ok := acc["streamers"]; ok && s != nil {
						streamers = s
					}
				}
				safe[label] = map[string]any{
					"username":  label,
					"userId":    "anonimo",
					"streamers": streamers,
				}
			}
			status["statuses"] = safe
		}
		if logs, ok := status["logs"].([]any); ok {
			for i, l := range logs {
				if s, ok := l.(string); ok {
					s = stealth.NickRE.ReplaceAllLiteralString(s, "Minerador #1")
					s = reIDLogin.ReplaceAllLiteralString(s, "(Sessão Autenticada)")
					logs[i] = s
				}
			}
		}
	}
	return stealth.HubDeep().Apply(bots)
}

func buildBots(results map[string]map[string]any) map[string]any {
	return map[string]any{
		"huntera": map[string]any{
			"name":         "Huntera Idle Bot",
			"game":         "Huntera",
			"engine":       "Go Native Engine",
			"port":         3000,
			"dashboardUrl": "https://kbelludoo.github.io/huntera-bot/",
			"status":       results["huntera"],
		},
		"tibidle": map[string]any{
			"name":         "Tibidle Multi-Bot",
			"game":         "Tibidle",
			"engine":       "Go Native Engine",
			"port":         3001,
			"dashboardUrl": "https://kbelludoo.github.io/tibidle-bot/",
			"status":       results["tibidle"],
		},
		"sigma": map[string]any{
			"name":         "Pokemon Sigma Multi-Bot",
			"game":         "Pokemon Sigma",
			"engine":       "Go Native Engine",
			"port":         3002,
			"dashboardUrl": "https://kbelludoo.github.io/sigma-bot/",
			"status":       results["sigma"],
		},
		"leveling": map[string]any{
			"name":         "Leveling Idle 2D Bot",
			"game":         "Leveling Idle 2D",
			"engine":       "Go Native Engine (Protobuf 3)",
			"port":         3003,
			"dashboardUrl": "https://kbelludoo.github.io/leveling-bot/",
			"status":       results["leveling"],
		},
		"pokeidle": map[string]any{
			"name":         "PokeIdle.io Bot",
			"game":         "PokeIdle.io",
			"engine":       "Go Native Engine (Twitch Lurker)",
			"port":         3006,
			"dashboardUrl": "https://kbelludoo.github.io/pokeidle-go/",
			"status":       results["pokeidle"],
		},
		"pokeidleworld": map[string]any{
			"name":         "Poke Idle World Bot",
			"game":         "Poke Idle World",
			"engine":       "Go Native Engine (Auto-Catch)",
			"port":         3007,
			"dashboardUrl": "https://kbelludoo.github.io/pokeidleworld-bot/",
			"status":       results["pokeidleworld"],
		},
		"kickminer": map[string]any{
			"name":         "Kick Channel Points Miner",
			"game":         "Kick.com",
			"engine":       "Go Native (Zero-Video WS)",
			"port":         3009,
			"dashboardUrl": "https://kbelludoo.github.io/kick-miner-go/",
			"status":       results["kickminer"],
		},
		"twitchminer": map[string]any{
			"name":         "Twitch Channel Points Miner",
			"game":         "Twitch.tv",
			"engine":       "Go Native (Zero-Video WS)",
			"port":         3010,
			"dashboardUrl": "https://kbelludoo.github.io/twitch-miner-go/",
			"status":       results["twitchminer"],
		},
		"baiak": map[string]any{
			"name":         "Baiak Idle Multi-Account",
			"game":         "Baiak Idle",
			"engine":       "Go Native Engine",
			"port":         8090,
			"dashboardUrl": "https://kbelludoo.github.io/baiak-idle-bot/",
			"status":       results["baiak"],
		},
		"lolidle": map[string]any{
			"name":         "LoL Idle Multi-Bot",
			"game":         "LoL Idle",
			"engine":       "Go Native Engine (Runeterra 4x Party)",
			"port":         3011,
			"dashboardUrl": "https://kbelludoo.github.io/lolidle-bot/",
			"status":       results["lolidle"],
		},
	}
}

func countActive(results map[string]map[string]any) int {
	active := 0
	for _, key := range []string{"huntera", "tibidle", "sigma", "leveling", "pokeidle", "pokeidleworld", "kickminer", "lolidle"} {
		if isOnline(results[key]) {
			active++
		}
	}
	tm := results["twitchminer"]
	if tm != nil && (isOnline(tm) || len(mustMap(tm["statuses"])) > 0) {
		active++
	}
	bk := results["baiak"]
	if isOnline(bk) && (numGT(nestedVal(bk, "fleet", "botsOnline"), 0) || isOnline(bk)) {
		active++
	}
	return active
}

func mustMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func nestedVal(m map[string]any, keys ...string) any {
	cur := m
	for i, k := range keys {
		v, ok := cur[k].(map[string]any)
		if !ok {
			if i == len(keys)-1 {
				return cur[k]
			}
			return nil
		}
		cur = v
	}
	return cur
}

func main() {
	push := false
	for _, a := range os.Args[1:] {
		if a == "--push" {
			push = true
		}
	}

	vps := vpsmetrics.Collect()
	results := fetchAll()
	active := countActive(results)
	bots := buildBots(results)

	payload := map[string]any{
		"timestamp": time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		"vps":       vps,
		"summary": map[string]any{
			"totalBots":  len(results),
			"activeBots": active,
		},
		"bots": sanitizeBotPayload(bots),
	}

	scrubbed := stealth.Hub().Apply(payload)
	jsonBytes, err := stealth.Marshal(scrubbed)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[-] Falha ao serializar payload: %v\n", err)
		os.Exit(1)
	}

	if !push {
		os.Stdout.Write(jsonBytes)
		return
	}

	sha, err := ghsync.FileSHA("bot-template-go", "docs/status.json")
	if err != nil {
		sha = ""
	}
	if err := ghsync.Put("bot-template-go", "docs/status.json", "telemetry: live vps 8 bots summary", jsonBytes, sha); err != nil {
		fmt.Fprintf(os.Stderr, "[-] Falha ao sincronizar via GitHub API: %v\n", err)
		return
	}
	fmt.Println("✓ Resumo da VPS (8 bots ativos) sincronizado com sucesso no GitHub Pages!")
}
