/*
* FICHERO: gestor.go
* DESCRIPCIÓN:  Ofrece las dos
*              operaciones del guion, ya sincronizadas con Ricart-Agrawala.
* AUTORES: Anastasia Shlyk, Enrique José Guaras Lacasta
 */
package gestor

import (
	"log"
	"os"
	"practica2/ra"
)

type Gestor struct {
	ra      *ra.RASharedDB
	pid     int
	op      ra.Operation // rol fijo de este proceso: ra.Read o ra.Write
	fichero string       // ruta de mi copia local
}

func New(pid int, usersFile string, fichero string, op ra.Operation) *Gestor {
	return &Gestor{
		ra:      ra.New(pid, usersFile, int(op), fichero),
		pid:     pid,
		op:      op,
		fichero: fichero,
	}
}

func (g *Gestor) LeerFichero() string {
	g.ra.PreProtocol()
	log.Printf("P%d ENTRA lectura", g.pid)

	b, err := os.ReadFile(g.fichero)
	if err != nil {
		log.Printf("P%d error leyendo %s: %v", g.pid, g.fichero, err)
	}

	log.Printf("P%d SALE lectura", g.pid)
	g.ra.PostProtocol()
	return string(b)
}


func (g *Gestor) EscribirFichero(fragmento string) {
	if g.op != ra.Write {
		log.Fatalf("P%d: EscribirFichero en un proceso que no es escritor", g.pid)
	}
	g.ra.PreProtocol()
	log.Printf("P%d ENTRA escritura %q", g.pid, fragmento)

	g.ra.EscribirEnTodasCopias(fragmento)

	log.Printf("P%d SALE escritura %q", g.pid, fragmento)
	g.ra.PostProtocol()
}

// Barrera final: no vuelve hasta que todos los procesos han terminado.
func (g *Gestor) Stop() {
	g.ra.Stop()
}