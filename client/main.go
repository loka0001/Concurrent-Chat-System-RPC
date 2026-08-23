package main

import (
	"bufio"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"concurrentchatrpc/shared"
)

type Console struct {
	mu sync.Mutex
}

func (c *Console) Printf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Printf(format, args...)
}

type ClientReceiver struct {
	username string
	console  *Console
}

func (r *ClientReceiver) Deliver(event shared.Event, _ *struct{}) error {
	switch event.Kind {
	case "message":
		r.console.Printf("[%s] %s: %s\n", r.username, event.From, event.Text)
	case "join", "leave":
		r.console.Printf("[%s] %s\n", r.username, event.Text)
	default:
		r.console.Printf("[%s] %s\n", r.username, event.Text)
	}
	return nil
}

func startCallbackServer(receiver *ClientReceiver) (net.Listener, string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", err
	}

	callbackServer := rpc.NewServer()
	if err := callbackServer.RegisterName("ClientReceiver", receiver); err != nil {
		_ = listener.Close()
		return nil, "", err
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go callbackServer.ServeConn(conn)
		}
	}()

	return listener, listener.Addr().String(), nil
}

func printHelp(console *Console) {
	console.Printf("Commands:\n")
	console.Printf("  send <message>       Send a message to all other connected clients\n")
	console.Printf("  users                List all connected users\n")
	console.Printf("  who                  Show this client's username\n")
	console.Printf("  help                 Show this help text\n")
	console.Printf("  quit / exit          Leave the chat and exit\n")
}

func main() {
	const serverAddress = "127.0.0.1:12345"

	console := &Console{}
	scanner := bufio.NewScanner(os.Stdin)

	console.Printf("Username: ")
	if !scanner.Scan() {
		return
	}
	username := strings.TrimSpace(scanner.Text())
	if username == "" || strings.ContainsAny(username, " \t") {
		console.Printf("error: username must be one non-empty word\n")
		return
	}

	receiver := &ClientReceiver{username: username, console: console}
	callbackListener, callbackAddr, err := startCallbackServer(receiver)
	if err != nil {
		console.Printf("error: cannot start callback RPC server: %v\n", err)
		return
	}
	defer callbackListener.Close()

	server, err := rpc.Dial("tcp", serverAddress)
	if err != nil {
		console.Printf("error: cannot connect to chat server at %s: %v\n", serverAddress, err)
		return
	}
	defer server.Close()

	var joinReply string
	if err := server.Call("Chat.Join", shared.JoinArgs{
		Username:     username,
		CallbackAddr: callbackAddr,
	}, &joinReply); err != nil {
		console.Printf("error: %v\n", err)
		return
	}
	console.Printf("%s\n", joinReply)

	var leaveOnce sync.Once
	leave := func() {
		leaveOnce.Do(func() {
			var reply string
			if err := server.Call("Chat.Leave", shared.LeaveArgs{Username: username}, &reply); err == nil {
				console.Printf("%s\n", reply)
			}
		})
	}
	defer leave()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	go func() {
		<-signals
		console.Printf("\nLeaving chat...\n")
		leave()
		_ = os.Stdin.Close()
	}()

	printHelp(console)

	for {
		console.Printf("> ")
		if !scanner.Scan() {
			return
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 2)
		command := strings.ToLower(parts[0])
		argument := ""
		if len(parts) == 2 {
			argument = strings.TrimSpace(parts[1])
		}

		switch command {
		case "send":
			if argument == "" {
				console.Printf("error: usage: send <message>\n")
				continue
			}

			var reply string
			if err := server.Call("Chat.Send", shared.SendArgs{
				From: username,
				Text: argument,
			}, &reply); err != nil {
				console.Printf("error: %v\n", err)
				continue
			}
			console.Printf("%s\n", reply)

		case "users":
			var reply shared.UsersReply
			if err := server.Call("Chat.Users", shared.UsersArgs{}, &reply); err != nil {
				console.Printf("error: %v\n", err)
				continue
			}
			for _, name := range reply.Users {
				if name == username {
					console.Printf("* %s\n", name)
				} else {
					console.Printf("  %s\n", name)
				}
			}

		case "who":
			console.Printf("acting as: %s\n", username)

		case "help":
			printHelp(console)

		case "quit", "exit":
			leave()
			return

		default:
			console.Printf("error: unknown command %q. Type 'help' for available commands.\n", command)
		}
	}
}
