package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"

	"concurrentchatrpc/shared"
)

type clientSession struct {
	username string
	callback *rpc.Client
}

type ChatService struct {
	mu      sync.RWMutex
	clients map[string]*clientSession
}

func NewChatService() *ChatService {
	return &ChatService{clients: make(map[string]*clientSession)}
}

func (s *ChatService) Join(args shared.JoinArgs, reply *string) error {
	username := strings.TrimSpace(args.Username)
	if username == "" {
		return fmt.Errorf("username cannot be empty")
	}
	if strings.TrimSpace(args.CallbackAddr) == "" {
		return fmt.Errorf("callback address cannot be empty")
	}

	callback, err := rpc.Dial("tcp", args.CallbackAddr)
	if err != nil {
		return fmt.Errorf("cannot connect to client callback: %w", err)
	}

	s.mu.Lock()
	if _, exists := s.clients[username]; exists {
		s.mu.Unlock()
		_ = callback.Close()
		return fmt.Errorf("username %q is already connected", username)
	}

	recipients := make([]*clientSession, 0, len(s.clients))
	for _, client := range s.clients {
		recipients = append(recipients, client)
	}

	s.clients[username] = &clientSession{
		username: username,
		callback: callback,
	}
	s.mu.Unlock()

	s.broadcast(recipients, shared.Event{
		Kind: "join",
		Text: fmt.Sprintf("User %s joined the chat.", username),
	})

	*reply = fmt.Sprintf("%s has joined the chat.", username)
	log.Printf("%s joined", username)
	return nil
}

func (s *ChatService) Send(args shared.SendArgs, reply *string) error {
	text := strings.TrimSpace(args.Text)
	if text == "" {
		return fmt.Errorf("message cannot be empty")
	}

	s.mu.RLock()
	if _, exists := s.clients[args.From]; !exists {
		s.mu.RUnlock()
		return fmt.Errorf("user %q is not connected", args.From)
	}

	recipients := make([]*clientSession, 0, len(s.clients)-1)
	for username, client := range s.clients {
		if username != args.From {
			recipients = append(recipients, client)
		}
	}
	s.mu.RUnlock()

	s.broadcast(recipients, shared.Event{
		Kind: "message",
		From: args.From,
		Text: text,
	})

	*reply = fmt.Sprintf("%s: %s", args.From, text)
	return nil
}

func (s *ChatService) Leave(args shared.LeaveArgs, reply *string) error {
	s.mu.Lock()
	client, exists := s.clients[args.Username]
	if !exists {
		s.mu.Unlock()
		return fmt.Errorf("user %q is not connected", args.Username)
	}

	delete(s.clients, args.Username)
	recipients := make([]*clientSession, 0, len(s.clients))
	for _, remaining := range s.clients {
		recipients = append(recipients, remaining)
	}
	s.mu.Unlock()

	_ = client.callback.Close()

	s.broadcast(recipients, shared.Event{
		Kind: "leave",
		Text: fmt.Sprintf("User %s left the chat.", args.Username),
	})

	*reply = fmt.Sprintf("%s has left the chat.", args.Username)
	log.Printf("%s left", args.Username)
	return nil
}

func (s *ChatService) Users(_ shared.UsersArgs, reply *shared.UsersReply) error {
	s.mu.RLock()
	names := make([]string, 0, len(s.clients))
	for username := range s.clients {
		names = append(names, username)
	}
	s.mu.RUnlock()

	sort.Strings(names)
	reply.Users = names
	return nil
}

func (s *ChatService) snapshotClients() []*clientSession {
	s.mu.RLock()
	defer s.mu.RUnlock()

	clients := make([]*clientSession, 0, len(s.clients))
	for _, client := range s.clients {
		clients = append(clients, client)
	}
	return clients
}

func (s *ChatService) broadcast(recipients []*clientSession, event shared.Event) {
	var wg sync.WaitGroup
	for _, recipient := range recipients {
		recipient := recipient
		wg.Add(1)
		go func() {
			defer wg.Done()
			var ack struct{}
			if err := recipient.callback.Call("ClientReceiver.Deliver", event, &ack); err != nil {
				log.Printf("push to %s failed: %v", recipient.username, err)
			}
		}()
	}
	wg.Wait()
}

func (s *ChatService) CloseAll() {
	clients := s.snapshotClients()

	s.mu.Lock()
	s.clients = make(map[string]*clientSession)
	s.mu.Unlock()

	for _, client := range clients {
		_ = client.callback.Close()
	}
}

func main() {
	const address = "127.0.0.1:12345"

	chat := NewChatService()
	rpcServer := rpc.NewServer()
	if err := rpcServer.RegisterName("Chat", chat); err != nil {
		log.Fatal(err)
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Chat RPC server listening on %s\n", address)
	fmt.Println("Press Ctrl+C to stop the server.")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		fmt.Println("\nShutting down server...")
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		go rpcServer.ServeConn(conn)
	}

	chat.CloseAll()
	signal.Stop(stop)
	fmt.Println("Server stopped.")
}
