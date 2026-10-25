package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type ActionRequest struct {
	AccountID string         `json:"accountId"`
	Action    string         `json:"action"`
	Payload   map[string]any `json:"payload"`
}

func main() {
	serverURL := flag.String("server", "http://127.0.0.1:3012", "URL do servidor do bot")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		return
	}

	command := args[0]
	client := &http.Client{Timeout: 10 * time.Second}

	switch command {
	case "status":
		resp, err := client.Get(*serverURL + "/api/status")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Erro ao obter status: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		var pretty bytes.Buffer
		_ = json.Indent(&pretty, body, "", "  ")
		fmt.Println(pretty.String())

	case "stop-hunt":
		if len(args) < 2 {
			fmt.Println("Uso: botctl stop-hunt <account-id>")
			os.Exit(1)
		}
		sendAction(client, *serverURL, args[1], "stopHunt", map[string]any{})

	case "start-hunt":
		if len(args) < 2 {
			fmt.Println("Uso: botctl start-hunt <account-id> [hunt-id]")
			os.Exit(1)
		}
		payload := map[string]any{}
		if len(args) >= 3 {
			payload["huntId"] = args[2]
		}
		sendAction(client, *serverURL, args[1], "startHunt", payload)

	case "toggle-hunt":
		if len(args) < 2 {
			fmt.Println("Uso: botctl toggle-hunt <account-id>")
			os.Exit(1)
		}
		sendAction(client, *serverURL, args[1], "toggleAutoHunt", map[string]any{})

	case "promote":
		if len(args) < 2 {
			fmt.Println("Uso: botctl promote <account-id>")
			os.Exit(1)
		}
		sendAction(client, *serverURL, args[1], "promote", map[string]any{})

	case "transfer":
		if len(args) < 4 {
			fmt.Println("Uso: botctl transfer <from-account-id> <to-char-name> <amount>")
			os.Exit(1)
		}
		var amt int64
		_, _ = fmt.Sscanf(args[3], "%d", &amt)
		sendAction(client, *serverURL, args[1], "transfer", map[string]any{
			"name":   args[2],
			"amount": amt,
		})

	case "market":
		if len(args) < 2 {
			fmt.Println("Uso: botctl market <account-id>")
			os.Exit(1)
		}
		sendAction(client, *serverURL, args[1], "coinMarket", map[string]any{"pagina": 1})

	case "buy-coins":
		if len(args) < 4 {
			fmt.Println("Uso: botctl buy-coins <account-id> <quantidade-coins> <preco-por-coin-em-gold>")
			os.Exit(1)
		}
		var amt, price int64
		_, _ = fmt.Sscanf(args[2], "%d", &amt)
		_, _ = fmt.Sscanf(args[3], "%d", &price)
		sendAction(client, *serverURL, args[1], "buyCoins", map[string]any{
			"amount": amt,
			"price":  price,
		})

	case "action":
		if len(args) < 3 {
			fmt.Println("Uso: botctl action <account-id> <action-name>")
			os.Exit(1)
		}
		sendAction(client, *serverURL, args[1], args[2], map[string]any{})

	default:
		printUsage()
	}
}

func sendAction(client *http.Client, serverURL, accountID, action string, payload map[string]any) {
	reqData := ActionRequest{
		AccountID: accountID,
		Action:    action,
		Payload:   payload,
	}
	jsonData, _ := json.Marshal(reqData)

	resp, err := client.Post(serverURL+"/api/action", "application/json", bytes.NewReader(jsonData))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Erro ao enviar ação: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Println(string(body))
}

func printUsage() {
	fmt.Println("Uso do botctl (Utilitário Nativo em Go):")
	fmt.Println("  botctl status                                        - Exibe o status de todas as contas")
	fmt.Println("  botctl stop-hunt <account-id>                        - Encerra a caçada de uma conta")
	fmt.Println("  botctl start-hunt <account-id> [hunt-id]             - Inicia a caçada (com mapa opcional)")
	fmt.Println("  botctl toggle-hunt <account-id>                      - Liga/desliga o auto-hunt da conta")
	fmt.Println("  botctl promote <account-id>                          - Envia comando de promoção de vocação")
	fmt.Println("  botctl transfer <account-id> <dest-name> <amount>    - Transfere ouro entre personagens")
	fmt.Println("  botctl market <account-id>                           - Consulta cotação de Lux Coins no mercado")
	fmt.Println("  botctl buy-coins <account-id> <quant> <preco_ouro>   - Envia ordem de compra de Lux Coins com ouro")
	fmt.Println("  botctl action <account-id> <action-name>             - Dispara ação arbitrária no bot")
}
