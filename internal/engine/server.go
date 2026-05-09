package engine

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)

type OpType string

const (
	GET    OpType = "GET"
	SET    OpType = "SET"
	DELETE OpType = "DELETE"
)

type Reuest struct {
	Method OpType
	Key    int64
	Value  int64
}

type Server struct {
	listenAddr string
	ln         net.Listener
	done       chan struct{}
	Engine     *Engine
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		log.Println("Error on starting server")
		return err
	}

	s.ln = ln
	go s.serverLoop()
	<-s.done
	select {}
}

func (s *Server) Stop() {
	close(s.done)
	s.ln.Close()
}

func NewServer(listenAddr string) *Server {
	return &Server{
		listenAddr: listenAddr,
		done:       make(chan struct{}),
	}
}

func (s *Server) serverLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			log.Println("Accept Connection Failed:", err)
		}

		log.Println("Accepted Connection:", conn.RemoteAddr())

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Read error: %v", err)
			return
		}

		request, err := s.parseRequest(message)
		if err != nil {
			response := fmt.Sprintf("%v", err)
			_, err = conn.Write([]byte(response))
			conn.Close()
		}
		var response string
		if request.Method == GET {
			response = fmt.Sprintf("%d", s.Engine.Get(request.Key))
		}
		if request.Method == SET {
			err := s.Engine.Insert(request.Key, request.Value)
			if err != nil {
				response = fmt.Sprintf("%v", err)
			}
		}
		if request.Method == DELETE {
			err = s.Engine.Delete(request.Key)
			if err != nil {
				response = fmt.Sprintf("%v", err)
			}
		}

		_, err = conn.Write([]byte(response + "\n"))
		if err != nil {
			log.Printf("Server write error: %v", err)
			return
		}
	}
}

func (s *Server) parseRequest(request string) (Reuest, error) {
	words := strings.Fields(request)
	if len(words) == 0 {
		return Reuest{}, fmt.Errorf("empty request")
	}

	command := strings.ToUpper(strings.TrimSpace(words[0]))
	if len(words) > 3 {
		return Reuest{}, fmt.Errorf("command not found or too much parameter")
	}

	switch command {
	case string(GET):
		if len(words) != 2 {
			return Reuest{}, fmt.Errorf("invalid GET request")
		}
		key, err := strconv.ParseInt(words[1], 10, 64)
		if err != nil {
			return Reuest{}, fmt.Errorf("invalid GET key: %w", err)
		}
		return Reuest{Method: GET, Key: key}, nil
	case string(SET):
		if len(words) != 3 {
			return Reuest{}, fmt.Errorf("invalid SET request")
		}
		key, err := strconv.ParseInt(words[1], 10, 64)
		if err != nil {
			return Reuest{}, fmt.Errorf("invalid SET key: %w", err)
		}
		val, err := strconv.ParseInt(words[2], 10, 64)
		if err != nil {
			return Reuest{}, fmt.Errorf("invalid SET value: %w", err)

		}
		return Reuest{Method: SET, Key: key, Value: val}, nil
	case string(DELETE):
		if len(words) != 2 {
			return Reuest{}, fmt.Errorf("invalid DELETE request")
		}
		key, err := strconv.ParseInt(words[1], 10, 64)
		if err != nil {
			return Reuest{}, fmt.Errorf("invalid DELETE key: %w", err)
		}
		return Reuest{Method: DELETE, Key: key}, nil
	default:
		return Reuest{}, fmt.Errorf("command not found")
	}
}
