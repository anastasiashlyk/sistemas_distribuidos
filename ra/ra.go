/*
* AUTOR: Rafael Tolosana Calasanz
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: septiembre de 2021
* FICHERO: ricart-agrawala.go
* DESCRIPCIÓN: Implementación del algoritmo de Ricart-Agrawala Generalizado en Go
 */
package ra

import (
	"bufio"
	"os"
	"practica2/ms"
	"sync"
)


type Operation int

const (
		Read Operation = iota
		Write
)

var exclude = [2][2]bool{
	//Read   Write
	{false, true},  // Read
	{true, true},  // Write
}


type Request struct{
    Clock   int
    Pid     int
		Op Operation
}

type Reply struct{
		Pid int
}

type Escritura struct{
		From int
		Seq int
		Fragment string
}

// Confirmación de que una copia ya ha aplicado la escritura
type EscrituraAck struct {
	Pid int
}

type Fin struct {
	Pid int
}


type RASharedDB struct {
    OurSeqNum   int       // MI  reloj actual
    HigSeqNum   int				//el mayot numero de reloj visto hasta ahora
    OutRepCnt   int				// permisos que me faltan para entrar en SC

    ReqCS       bool		// true si estoy pidiendo entrar en SC
    RepDefd     []bool 	// pendientes de responder a los procesos 
    ms          *ms.MessageSystem

    done        chan bool
    chrep       chan bool // canal para avisar que se ha recibido todos los permisos
		chack chan bool  // acks de escritura recibidos
		chfin chan bool  // fin de ejecución recibido

    Mutex       sync.Mutex // mutex para proteger concurrencia sobre las variables
    // TODO: completar

		mifichero string // ruta de mi fichero
		sequenceEscritura int // secuencia de escritura
		yaAplicado map[int]int // mapa de secuencias de escrituras ya aplicadas por proceso

		me int
		numProcesos int
		op Operation // mi operacion
}


func New(me int, usersFile string, op int, mifichero string) (*RASharedDB) {
    messageTypes := []ms.Message{Request{}, Reply{}, Escritura{}, EscrituraAck{}, Fin{}}
    msgs := ms.New(me, usersFile, messageTypes)
		n := contarLineas(usersFile)
		f, _ := os.OpenFile(mifichero, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		f.Close()
    ra := &RASharedDB{
			RepDefd:     make([]bool, n+1), // posiciones 1..n
			ms:          &msgs,
			done:        make(chan bool, 1),
			chrep:       make(chan bool, 1),
			chack:       make(chan bool, n),
			chfin:       make(chan bool, n),
			me:          me,

			numProcesos: n,
			op:          Operation(op),
			mifichero:   mifichero,
			yaAplicado:  make(map[int]int),
	}
    // TODO completar
		go ra.recibirMensajes()
    return ra
}

func (ra *RASharedDB) recibirMensajes() {
	for {
		msg := ra.ms.Receive()
		switch m := msg.(type) {
		case Request:
			ra.handleRequest(m)
		case Reply:
			ra.handleReply(m)
		case Escritura: 
			ra.handleEscritura(m)
		case EscrituraAck:
			ra.chack <- true
		case Fin:
			ra.chfin <- true
		
		}
	}
}

func (ra *RASharedDB) handleRequest(r Request) {
	ra.Mutex.Lock()
	if r.Clock > ra.HigSeqNum {
		ra.HigSeqNum = r.Clock
	}
	miPrioridad := ra.OurSeqNum < r.Clock ||
		(ra.OurSeqNum == r.Clock && ra.me < r.Pid)
	aplazar := ra.ReqCS && exclude[ra.op][r.Op] && miPrioridad
	if aplazar {
		ra.RepDefd[r.Pid] = true
	}
	ra.Mutex.Unlock()
 
	if !aplazar { 
		ra.ms.Send(r.Pid, Reply{Pid: ra.me})
	}
}


func (ra *RASharedDB) handleReply(r Reply) {
	ra.Mutex.Lock()
	ra.OutRepCnt--
	todos := ra.OutRepCnt == 0
	ra.Mutex.Unlock()
	if todos {
		ra.chrep <- true
	}
}


func (ra *RASharedDB) handleEscritura(w Escritura) {
	ra.Mutex.Lock()
	if w.Seq > ra.yaAplicado[w.From] && ra.mifichero != "" && w.Fragment != "" {
		if f, err := os.OpenFile(ra.mifichero, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
			_, _ = f.WriteString(w.Fragment)
			_ = f.Close()
			ra.yaAplicado[w.From] = w.Seq
		}
	}
	ra.Mutex.Unlock()

	ra.ms.Send(w.From, EscrituraAck{Pid: ra.me})
}

//Pre: Verdad
//Post: Realiza  el  PreProtocol  para el  algoritmo de
//      Ricart-Agrawala Generalizado
func (ra *RASharedDB) PreProtocol(){
    // TODO completar
		ra.Mutex.Lock()
		ra.ReqCS = true
		// incrementar mi reloj 
		ra.OurSeqNum = ra.HigSeqNum + 1
		ra.OutRepCnt = ra.numProcesos - 1
		seq := ra.OurSeqNum
		ra.Mutex.Unlock()

		// enviar peticiones a todos los procesos
		for pid := 1; pid <= ra.numProcesos; pid++ {
			if pid != ra.me {
				req := Request{Clock: seq, Pid: ra.me, Op: ra.op}

				ra.ms.Send(pid, req)
			}
		}
		// esperar a recibir todos los permisos
		if ra.numProcesos > 1 {
			<-ra.chrep
		}
		
}

//Pre: Verdad
//Post: Realiza  el  PostProtocol  para el  algoritmo de
//      Ricart-Agrawala Generalizado
func (ra *RASharedDB) PostProtocol(){
    // TODO completar
	ra.Mutex.Lock()
	ra.ReqCS = false
	pendientes := []int{}
	for pid := 1; pid <= ra.numProcesos; pid++ {
		if ra.RepDefd[pid] {
			pendientes = append(pendientes, pid)
			ra.RepDefd[pid] = false
		}
	}
	ra.Mutex.Unlock()
 
	for _, pid := range pendientes {
		ra.ms.Send(pid, Reply{Pid: ra.me})
	}
}

// Barrera final: avisa a todos de que he terminado y espera a que todos
// hayan terminado. Hasta entonces sigo atendiendo peticiones ajenas.
// Llamar cuando ya no se va a pedir más la SC.
func (ra *RASharedDB) Stop() {
	for pid := 1; pid <= ra.numProcesos; pid++ {
		if pid != ra.me {
			ra.ms.Send(pid, Fin{Pid: ra.me})
		}
	}
	for i := 0; i < ra.numProcesos-1; i++ {
		<-ra.chfin
	}
	// El proceso termina cuando main retorna.
	ra.done <- true
}


func contarLineas(ruta string) int {
	f, err := os.Open(ruta)
	if err != nil {
		return 1
	}
	defer f.Close()
 
	n := 0
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		if scan.Text() != "" {
			n++
		}
	}
	if n == 0 {
		n = 1
	}
	return n
}

func (ra *RASharedDB) EscribirEnTodasCopias(texto string) {
	if texto == "" {
		return
	}

	ra.Mutex.Lock()
	ra.sequenceEscritura++
	seq := ra.sequenceEscritura
	ra.Mutex.Unlock()

		for pid := 1; pid <= ra.numProcesos; pid++ {
				// ojo, lo envio a mi mismo tambien, para que se aplique en mi copia local
				ra.ms.Send(pid, Escritura{From: ra.me, Seq: seq, Fragment: texto})
			
		}

		for i := 0; i < ra.numProcesos; i++ {
			<-ra.chack
		}

}