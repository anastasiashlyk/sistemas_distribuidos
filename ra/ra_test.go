package ra_test

import (
	"fmt"
	"log"
	"net"
	"os"
	"practica2/gestor"
	"practica2/ra"
	"strings"
	"sync"
	"testing"
	"time"
)

func init() {
	// Configuración de flags de logging exigida en guion (sección 4.2)
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
}

// creaUsersTemp crea un archivo users temporal con N puertos locales libres
func creaUsersTemp(t *testing.T, n int) string {
	t.Helper()
	var addrs []string
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Error obteniendo puerto: %v", err)
		}
		addrs = append(addrs, l.Addr().String())
		l.Close()
	}

	tmp, err := os.CreateTemp("", "users_ra_*.txt")
	if err != nil {
		t.Fatalf("Error creando fichero temporal: %v", err)
	}
	defer tmp.Close()

	for _, a := range addrs {
		tmp.WriteString(a + "\n")
	}
	time.Sleep(20 * time.Millisecond)
	return tmp.Name()
}

// opsTodas devuelve n veces la misma operación
func opsTodas(n int, op ra.Operation) []ra.Operation {
	ops := make([]ra.Operation, n)
	for i := range ops {
		ops[i] = op
	}
	return ops
}

// creaNodos crea un nodo RA por cada operación de ops (el nodo i+1 tiene
// el rol ops[i], que en tu ra.go queda fijado en New). No tienen fichero.
func creaNodos(users string, ops []ra.Operation) []*ra.RASharedDB {
	nodos := make([]*ra.RASharedDB, len(ops))
	for i := range ops {
		nodos[i] = ra.New(i+1, users, int(ops[i]), "")
	}
	time.Sleep(100 * time.Millisecond) // que todos estén escuchando
	return nodos
}

func stopsRA(nodos []*ra.RASharedDB) []func() {
	var f []func()
	for _, n := range nodos {
		f = append(f, n.Stop)
	}
	return f
}

// Stop() es una BARRERA: no vuelve hasta que todos los procesos lo han
// llamado. Por eso hay que llamarlo para todos a la vez, nunca en secuencia
// (un defer por nodo se bloquearía en el primero).
func detener(t *testing.T, stops []func()) {
	var wg sync.WaitGroup
	for _, s := range stops {
		wg.Add(1)
		go func(f func()) {
			defer wg.Done()
			f()
		}(s)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Error("la barrera final (Stop) no terminó")
	}
}

// esperar hace wg.Wait() con timeout para detectar interbloqueos
func esperar(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("posible interbloqueo: el test no termina")
	}
}

// TestLectoresConcurrentes comprueba que varios lectores acceden simultáneamente a SC
func TestLectoresConcurrentes(t *testing.T) {
	n := 3
	users := creaUsersTemp(t, n)
	defer os.Remove(users)

	nodos := creaNodos(users, opsTodas(n, ra.Read))
	defer detener(t, stopsRA(nodos))

	var mu sync.Mutex
	activos := 0
	maxSimul := 0
	var wg sync.WaitGroup
	barrera := make(chan struct{})

	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(id int, nodo *ra.RASharedDB) {
			defer wg.Done()
			<-barrera

			nodo.PreProtocol()

			mu.Lock()
			activos++
			if activos > maxSimul {
				maxSimul = activos
			}
			mu.Unlock()

			time.Sleep(50 * time.Millisecond)

			mu.Lock()
			activos--
			mu.Unlock()

			nodo.PostProtocol()
		}(i, nodos[i-1])
	}

	close(barrera)
	esperar(t, &wg, 20*time.Second)

	if maxSimul < 2 {
		t.Errorf("Se esperaba concurrencia real entre lectores, max simultáneos: %d", maxSimul)
	}
}

// TestEscritoresExclusionMutua comprueba que nunca coinciden dos escritores en SC
func TestEscritoresExclusionMutua(t *testing.T) {
	n := 3
	users := creaUsersTemp(t, n)
	defer os.Remove(users)

	nodos := creaNodos(users, opsTodas(n, ra.Write))
	defer detener(t, stopsRA(nodos))

	var mu sync.Mutex
	activos := 0
	solapados := false
	var wg sync.WaitGroup
	barrera := make(chan struct{})

	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(id int, nodo *ra.RASharedDB) {
			defer wg.Done()
			<-barrera

			nodo.PreProtocol()

			mu.Lock()
			activos++
			if activos > 1 {
				solapados = true
			}
			mu.Unlock()

			time.Sleep(30 * time.Millisecond)

			mu.Lock()
			activos--
			mu.Unlock()

			nodo.PostProtocol()
		}(i, nodos[i-1])
	}

	close(barrera)
	esperar(t, &wg, 20*time.Second)

	if solapados {
		t.Errorf("Error: Dos escritores entraron simultáneamente en Sección Crítica")
	}
}

// TestMixtoLectoresEscritores comprueba que un escritor en SC excluye a todos los demás
func TestMixtoLectoresEscritores(t *testing.T) {
	n := 4
	users := creaUsersTemp(t, n)
	defer os.Remove(users)

	// P1 y P2 lectores; P3 y P4 escritores (el rol se fija al crear el nodo)
	roles := []ra.Operation{ra.Read, ra.Read, ra.Write, ra.Write}
	nodos := creaNodos(users, roles)
	defer detener(t, stopsRA(nodos))

	var mu sync.Mutex
	lectores := 0
	escritores := 0
	falloExclusion := false
	var wg sync.WaitGroup
	barrera := make(chan struct{})

	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(id int, nodo *ra.RASharedDB, op ra.Operation) {
			defer wg.Done()
			<-barrera

			nodo.PreProtocol()

			mu.Lock()
			if op == ra.Read {
				lectores++
				if escritores > 0 {
					falloExclusion = true
				}
			} else {
				escritores++
				if escritores > 1 || lectores > 0 {
					falloExclusion = true
				}
			}
			mu.Unlock()

			time.Sleep(30 * time.Millisecond)

			mu.Lock()
			if op == ra.Read {
				lectores--
			} else {
				escritores--
			}
			mu.Unlock()

			nodo.PostProtocol()
		}(i, nodos[i-1], roles[i-1])
	}

	close(barrera)
	esperar(t, &wg, 20*time.Second)

	if falloExclusion {
		t.Errorf("Error: Fallo de exclusión mutua en escenario mixto (lector y escritor a la vez en SC)")
	}
}

// TestGestorFicherosConsistencia valida el gestor de ficheros (sección 1 del guion):
//   EscribirFichero propaga el fragmento a todas las réplicas.
//   LeerFichero devuelve el contenido de la réplica local.
//   Al finalizar, todas las copias tienen exactamente el mismo contenido.
// LeerFichero y EscribirFichero son ahora del paquete gestor (no de ra).
func TestGestorFicherosConsistencia(t *testing.T) {
	n := 4
	users := creaUsersTemp(t, n)
	defer os.Remove(users)

	// Crear 4 réplicas temporales
	var archivos []string
	for i := 1; i <= n; i++ {
		tmp, err := os.CreateTemp("", fmt.Sprintf("test_replica_%d_*.txt", i))
		if err != nil {
			t.Fatalf("error creando replica %v", err)
		}
		archivos = append(archivos, tmp.Name())
		tmp.Close()
		defer os.Remove(tmp.Name())
	}

	// P1 y P2 lectores; P3 y P4 escritores
	roles := []ra.Operation{ra.Read, ra.Read, ra.Write, ra.Write}
	gs := make([]*gestor.Gestor, n)
	var stops []func()
	for i := 0; i < n; i++ {
		gs[i] = gestor.New(i+1, users, archivos[i], roles[i])
		stops = append(stops, gs[i].Stop)
	}
	time.Sleep(100 * time.Millisecond) // que todos estén escuchando
	defer detener(t, stops)

	var wg sync.WaitGroup

	// P3 y P4 escriben usando EscribirFichero
	for _, pid := range []int{3, 4} {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for it := 1; it <= 3; it++ {
				gs[id-1].EscribirFichero(fmt.Sprintf("P%d_it%d\n", id, it))
				time.Sleep(10 * time.Millisecond)
			}
		}(pid)
	}

	// P1 y P2 leen concurrentemente usando LeerFichero
	for _, pid := range []int{1, 2} {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for it := 1; it <= 3; it++ {
				_ = gs[id-1].LeerFichero()
				time.Sleep(15 * time.Millisecond)
			}
		}(pid)
	}

	esperar(t, &wg, 30*time.Second)
	// No hace falta esperar a "que se apliquen los últimos updates": EscribirFichero
	// no vuelve hasta recibir el ack de todas las copias.

	// Comprobar que todas las réplicas tienen exactamente el mismo contenido
	data0, err := os.ReadFile(archivos[0])
	if err != nil {
		t.Fatalf("error leyendo replica 0: %v", err)
	}
	if len(data0) == 0 {
		t.Fatalf("la replica esta vacia tras las escrituras")
	}
	// 2 escritores x 3 iteraciones = 6 fragmentos, ninguno perdido
	if l := strings.Count(string(data0), "\n"); l != 6 {
		t.Errorf("se esperaban 6 fragmentos en la réplica 1, hay %d", l)
	}

	for i := 1; i < n; i++ {
		dataI, err := os.ReadFile(archivos[i])
		if err != nil {
			t.Fatalf("Error leyendo replica %d: %v", i, err)
		}
		if string(data0) != string(dataI) {
			t.Errorf("replica %d difiere de la replica 1:\n[R1]: %s\n[R%d]: %s", i+1, string(data0), i+1, string(dataI))
		}
	}
}