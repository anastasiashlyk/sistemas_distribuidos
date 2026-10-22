package ra_test

import (
	"fmt"
	"log"
	"net"
	"os"
	"practica2/ra"
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

// TestLectoresConcurrentes comprueba que varios lectores acceden simultáneamente a SC
func TestLectoresConcurrentes(t *testing.T) {
	n := 3
	users := creaUsersTemp(t, n)
	defer os.Remove(users)

	nodos := make([]*ra.RASharedDB, n)
	for i := 1; i <= n; i++ {
		nodos[i-1] = ra.New(i, users)
		defer nodos[i-1].Stop()
	}
	time.Sleep(50 * time.Millisecond)

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

			nodo.PreProtocol(ra.Read)

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
	wg.Wait()

	if maxSimul < 2 {
		t.Errorf("Se esperaba concurrencia real entre lectores, max simultáneos: %d", maxSimul)
	}
}

// TestEscritoresExclusionMutua comprueba que nunca coinciden dos escritores en SC
func TestEscritoresExclusionMutua(t *testing.T) {
	n := 3
	users := creaUsersTemp(t, n)
	defer os.Remove(users)

	nodos := make([]*ra.RASharedDB, n)
	for i := 1; i <= n; i++ {
		nodos[i-1] = ra.New(i, users)
		defer nodos[i-1].Stop()
	}
	time.Sleep(50 * time.Millisecond)

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

			nodo.PreProtocol(ra.Write)

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
	wg.Wait()

	if solapados {
		t.Errorf("Error: Dos escritores entraron simultáneamente en Sección Crítica")
	}
}

// TestMixtoLectoresEscritores comprueba que un escritor en SC excluye a todos los demás
func TestMixtoLectoresEscritores(t *testing.T) {
	n := 4
	users := creaUsersTemp(t, n)
	defer os.Remove(users)

	nodos := make([]*ra.RASharedDB, n)
	for i := 1; i <= n; i++ {
		nodos[i-1] = ra.New(i, users)
		defer nodos[i-1].Stop()
	}
	time.Sleep(50 * time.Millisecond)

	var mu sync.Mutex
	lectores := 0
	escritores := 0
	falloExclusion := false
	var wg sync.WaitGroup
	barrera := make(chan struct{})

	// P1 y P2 lectores; P3 y P4 escritores
	roles := []ra.Operation{ra.Read, ra.Read, ra.Write, ra.Write}

	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(id int, nodo *ra.RASharedDB, op ra.Operation) {
			defer wg.Done()
			<-barrera

			nodo.PreProtocol(op)

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
	wg.Wait()

	if falloExclusion {
		t.Errorf("Error: Fallo de exclusión mutua en escenario mixto (lector y escritor a la vez en SC)")
	}
}

// TestGestorFicherosConsistencia valida el gestor de ficheros (Seccion 1 del guion):
// 		EscribirFichero propaga el fragmento a todas las replicas (lectores y escritores).
// 		LeerFichero devuelve el contenido de la replica local.
// 		Al finalizar las escrituras, todas las copias tienen exactamente el mismo contenido.
func TestGestorFicherosConsistencia(t *testing.T) {
	n := 4
	users := creaUsersTemp(t, n)
	defer os.Remove(users)

	// Crear 4 replicas temporales
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

	nodos := make([]*ra.RASharedDB, n)
	for i := 1; i <= n; i++ {
		nodos[i-1] = ra.New(i, users, archivos[i-1])
		defer nodos[i-1].Stop()
	}
	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup

	// P3 y P4 escriben usando EscribirFichero
	for _, pid := range []int{3, 4} {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for it := 1; it <= 3; it++ {
				nodos[id-1].EscribirFichero(fmt.Sprintf("P%d_it%d\n", id, it))
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
				_ = nodos[id-1].LeerFichero()
				time.Sleep(15 * time.Millisecond)
			}
		}(pid)
	}

	wg.Wait()
	time.Sleep(100 * time.Millisecond) // Tiempo para que se apliquen ultimos updates en disco

	// Comprobar que todas las replicas tienen exactamente el mismo contenido
	data0, err := os.ReadFile(archivos[0])
	if err != nil {
		t.Fatalf("error leyendo replica 0: %v", err)
	}
	if len(data0) == 0 {
		t.Fatalf("la replica esta vacia tras las escrituras")
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

