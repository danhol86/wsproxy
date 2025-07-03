// mautrix-wsproxy - A simple HTTP push -> websocket proxy for Matrix appservices.
// Copyright (C) 2021 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"maunium.net/go/mautrix/appservice"
)

type AckData struct {
	TxnID string `json:"txn_id"`
}

type queuedTxn struct {
	name       string
	logContent string
	payload    interface{}
	txnID      string
}

type AppService struct {
	ID string `yaml:"id"`
	AS string `yaml:"as"`
	HS string `yaml:"hs"`

	conn      *websocket.Conn          `yaml:"-"`
	connLock  sync.Mutex               `yaml:"-"`
	writeLock sync.Mutex               `yaml:"-"`
	ackLock   sync.Mutex               `yaml:"-"`
	acks      map[string]chan struct{} `yaml:"-"`

	queueLock sync.Mutex  `yaml:"-"`
	queue     []queuedTxn `yaml:"-"`
}

func (az *AppService) Conn() *websocket.Conn {
	return az.conn
}

func (az *AppService) createAck(txnID string) {
	ch := make(chan struct{})
	az.ackLock.Lock()
	az.acks[txnID] = ch
	az.ackLock.Unlock()
}

func (az *AppService) acknowledge(txnID string) {
	az.ackLock.Lock()
	if ch, ok := az.acks[txnID]; ok {
		close(ch)
		delete(az.acks, txnID)
	}
	az.ackLock.Unlock()
}

func (az *AppService) waitAck(txnID string, timeout time.Duration) bool {
	az.ackLock.Lock()
	ch, ok := az.acks[txnID]
	az.ackLock.Unlock()
	if !ok {
		return false
	}
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (az *AppService) enqueue(txn queuedTxn) {
	az.queueLock.Lock()
	az.queue = append(az.queue, txn)
	az.queueLock.Unlock()
}

func (az *AppService) sendQueued(txn queuedTxn) {
	az.createAck(txn.txnID)
	defer az.acknowledge(txn.txnID)
	for {
		conn := az.Conn()
		if conn == nil {
			time.Sleep(time.Second)
			continue
		}

		az.writeLock.Lock()
		log.Printf("Sending queued %s to %s containing %s", txn.name, az.ID, txn.logContent)
		err := conn.WriteJSON(txn.payload)
		az.writeLock.Unlock()
		if err != nil {
			log.Printf("Failed to send queued %s to %s: %v", txn.name, az.ID, err)
			time.Sleep(time.Second)
			continue
		}

		if az.waitAck(txn.txnID, 30*time.Second) {
			log.Printf("Sent queued %s to %s successfully", txn.name, az.ID)
			return
		}

		log.Printf("No ack for %s from %s, retrying", txn.name, az.ID)
	}
}

func (az *AppService) flushQueue() {
	for {
		az.queueLock.Lock()
		if len(az.queue) == 0 {
			az.queueLock.Unlock()
			return
		}
		txn := az.queue[0]
		az.queue = az.queue[1:]
		az.queueLock.Unlock()

		az.sendQueued(txn)
	}
}

const CloseConnReplaced = 4001

var upgrader = websocket.Upgrader{}

func syncWebsocket(w http.ResponseWriter, r *http.Request) {
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, "Bearer ") {
		errMissingToken.Write(w)
		return
	}
	az, ok := cfg.byASToken[authHeader[len("Bearer "):]]
	if !ok {
		errUnknownToken.Write(w)
		return
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Failed to upgrade websocket request:", err)
		return
	}
	log.Println(az.ID, "connected to websocket")
	defer func() {
		log.Println(az.ID, "disconnected from websocket")
		az.connLock.Lock()
		if az.conn == ws {
			az.conn = nil
			err := az.stopSyncProxy()
			if err != nil {
				log.Println("Error requesting syncproxy stop after", az.ID, "disconnected:", err)
			}
		}
		az.connLock.Unlock()
		az.ackLock.Lock()
		for id := range az.acks {
			delete(az.acks, id)
		}
		az.ackLock.Unlock()
		_ = ws.Close()
	}()
	err = ws.WriteMessage(websocket.TextMessage, []byte(`{"status": "connected"}`))
	if err != nil {
		log.Printf("Failed to write welcome status message to %s: %v", az.ID, err)
	}
	az.connLock.Lock()
	if az.conn != nil {
		go func(oldConn *websocket.Conn) {
			msg := websocket.FormatCloseMessage(CloseConnReplaced, `{"command": "disconnect", "status": "conn_replaced"}`)
			_ = oldConn.WriteControl(websocket.CloseMessage, msg, time.Now().Add(3*time.Second))
			_ = oldConn.Close()
		}(az.conn)
	}
	az.conn = ws
	az.connLock.Unlock()
	go az.flushQueue()
	for {
		var msg appservice.WebsocketCommand
		err = ws.ReadJSON(&msg)
		if err != nil {
			log.Println("Error reading from websocket:", err)
			break
		}
		go handleCommand(az, ws, &msg)
	}
}

func handleCommand(az *AppService, ws *websocket.Conn, msg *appservice.WebsocketCommand) {
	defer func() {
		panicErr := recover()
		if panicErr != nil {
			log.Printf("Panic while responding to command %s in request #%d: %v\n%s", msg.Command, msg.ReqID, panicErr, debug.Stack())
		}
	}()
	resp, err := actuallyHandleCommand(az, msg)
	if msg.ReqID != 0 {
		respPayload := appservice.WebsocketRequest{
			ReqID:   msg.ReqID,
			Command: "response",
			Data:    resp,
		}
		if err != nil {
			respPayload.Command = "error"
			respPayload.Data = map[string]interface{}{
				"message": err.Error(),
			}
		}
		az.writeLock.Lock()
		log.Printf("Sending response %+v", respPayload)
		err = ws.WriteJSON(&respPayload)
		az.writeLock.Unlock()
		if err != nil {
			log.Printf("Failed to send response to req #%d: %v", msg.ReqID, err)
		}
	}
}

type PingData struct {
	Timestamp int64 `json:"timestamp"`
}

func actuallyHandleCommand(az *AppService, msg *appservice.WebsocketCommand) (resp interface{}, err error) {
	defer func() {
		panicErr := recover()
		if panicErr != nil {
			log.Printf("Panic while handling command %s in request #%d: %v\n%s", msg.Command, msg.ReqID, panicErr, debug.Stack())
			if err == nil {
				err = fmt.Errorf("internal server error")
			}
		}
	}()
	switch msg.Command {
	case "start_sync":
		err = az.startSyncProxy(msg.Data)
		if err != nil {
			log.Println("Error forwarding", az.ID, "sync proxy start request:", err)
		}
	case "ack":
		var ack AckData
		if err = json.Unmarshal(msg.Data, &ack); err == nil {
			az.acknowledge(ack.TxnID)
		}
	case "ping":
		var req PingData
		jsonErr := json.Unmarshal(msg.Data, &req)
		now := time.Now()
		if req.Timestamp > 0 {
			pingStart := time.Unix(0, req.Timestamp*int64(time.Millisecond))
			log.Printf("Received ping from %s in %s", az.ID, now.Sub(pingStart))
		} else {
			log.Printf("Received ping from %s with no timestamp (json error: %v)", az.ID, jsonErr)
		}
		resp = &PingData{now.UnixNano() / int64(time.Millisecond)}
	default:
		log.Printf("Unknown command %s in request #%d from websocket. Data: %s", msg.Command, msg.ReqID, msg.Data)
		err = fmt.Errorf("unknown command %s", msg.Command)
	}
	return
}
