package cmds

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
)

var serverAddr string
var origin string

func SetServerAddr(addr string) {
	serverAddr = addr
}

func send(kind string, data any) error {
	corrID := fmt.Sprintf("%016x", rand.Uint64())

	head := map[string]any{"corrId": corrID}
	if origin != "" {
		head["resonate:origin"] = origin
	}
	req := map[string]any{
		"kind": kind,
		"head": head,
		"data": data,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	resp, err := http.Post(serverAddr, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		fmt.Fprintln(os.Stderr, string(raw))
		return nil
	}

	pretty, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(pretty))
	return nil
}
