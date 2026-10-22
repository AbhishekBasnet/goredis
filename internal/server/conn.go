package server

import (
	"bufio"
	"net"
)

func(s *Server) handle(conn net.Conn){
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	s.log.Info("Client connected", "remote", remote)
	defer s.log.Info("Client disconnected", "remote", remote)

	r:= bufio.NewReader(conn)
	for{
		if _,err := r.ReadString('\n'); err != nil {
			return
		}
		if _,err := conn.Write([]byte("+PONG\r\n")); err != nil {
			return
		}
	}

}