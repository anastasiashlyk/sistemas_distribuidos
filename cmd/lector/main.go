package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"practica2/ra"
	"strconv"
	"time"
)


func readEndpoints(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var endpoints []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			endpoints = append(endpoints, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return endpoints, nil
}



func barrera(usersFile string, myPid int, timeout time.Duration) error {
	endpoints, err := readEndpoints(usersFile)
	if err != nil {
		return err
	}

	deadline := time.Now().Add(timeout)
	for i, addr := range endpoints {
		pid := i + 1

		if pid == myPid {
			continue
		}
		for {
			if time.Now().After(deadline) {
				return fmt.Errorf("peer PID %d (%s) no escuchando a tiempo", pid, addr)
			}
			conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				break
			}
			time.Sleep(150 * time.Millisecond)
		}
	}
	return nil
}

func main() {

	if len(os.Args) != 4 {
		fmt.Println("Usage: go run ./cmd/lector <pid> <ficheroLocal> <fichParticipantes>")
		os.Exit(1)
	}

	miPid, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Invalid pid")
		os.Exit(1)
	}
	ficheroLocal := os.Args[2]
	fichParticipantes := os.Args[3]

	lector := ra.New(miPid, fichParticipantes, 0, ficheroLocal) // 0 para lectura

	//barrera, esperamos que todos esten listos
	err = barrera(fichParticipantes, miPid, 10*time.Second)
	if err != nil {
		fmt.Println("Error in barrier:", err)
		os.Exit(1)
	}

time.Sleep(1 * time.Second)

	fmt.Printf("Process %d starting pre-protocol\n", miPid)
	lector.PreProtocol()
	// leer el fichero
	bytes, err := os.ReadFile(ficheroLocal)
	if err != nil {
		fmt.Println("Error reading file:", err)
		os.Exit(1)
	}
	fmt.Printf("Process %d read from file %s:\n%s\n", miPid, ficheroLocal, string(bytes))
	//escribir contenido del fichero en la salida estandar
	fmt.Printf("Process %d contenido: %s\n", miPid, string(bytes))
	fmt.Printf("Process %d post-protocol", miPid)
	lector.PostProtocol()

	time.Sleep(1 * time.Second)
	lector.Stop()
	fmt.Printf("Process Lector %d finished\n", miPid)

}
