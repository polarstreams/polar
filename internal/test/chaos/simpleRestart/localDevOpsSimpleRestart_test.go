package simpleRestart

import (
	"fmt"
	dockerBuilder "github.com/helmutkemper/iotmaker.docker.builder"
	dockerBuilderNetwork "github.com/helmutkemper/iotmaker.docker.builder.network"
	"github.com/helmutkemper/util"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestLocalDevOpsSimpleRestart(t *testing.T) {
	var err error

	//if os.Getenv("enable_chaos_test") != "true" {
	//	return
	//}

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Defines the directory where the golang code runs
	err = os.Chdir("../../../../")
	if err != nil {
		panic(err)
	}

	// Install Barco on docker
	err = DockerSupport()
	if err != nil {
		panic(err)
	}
}

// DockerSupport
//
// Removes docker elements that may be left over from previous tests. Any docker element with the term "delete" in the name;
// Install a docker network at address 10.0.0.1 on the computer's network connector;
// Download and install Barco:latest in docker.
//
//	Note:
//	  * The Barco:latest image is not removed at the end of the tests so that they can be repeated more easily
func DockerSupport() (err error) {

	// Docker network controller object
	var netDocker *dockerBuilderNetwork.ContainerBuilderNetwork

	// English: Remove residual docker elements from previous tests (network, images, containers with the term `delete` in the name)
	// Português: Remove elementos docker residuais de testes anteriores (Rede, imagens, containers com o termo `delete` no nome)
	dockerBuilder.SaGarbageCollector()
	dockerBuilder.SaGarbageCollector("barco")

	dockerBuilder.ConfigChaosScene("barco", 1, 1, 2)

	// English: Create a docker network (as the gateway is 10.0.0.1, the first address will be 10.0.0.2)
	// Português: Cria uma rede docker (como o gateway é 10.0.0.1, o primeiro endereço será 10.0.0.2)
	netDocker, err = dockerTestNetworkCreate()
	if err != nil {
		return
	}

	var chanList = make([]<-chan dockerBuilder.Event, 3)
	var docker = make([]*dockerBuilder.ContainerBuilder, 3)
	for i := int64(0); i != 3; i += 1 {
		// English: Install Barco on docker
		// Português: Instala o Barco no docker

		docker[i] = new(dockerBuilder.ContainerBuilder)
		err = dockerBarco(netDocker, docker[i], i)
		if err != nil {
			err = fmt.Errorf("DockerSupport().error: the function dockerBarco() returned an error: %v", err)
			return
		}
	}

	for i := int64(0); i != 3; i += 1 {
		err = docker[i].ContainerStartAfterBuild()
		if err != nil {
			err = fmt.Errorf("DockerSupport().error: the function ContainerStartAfterBuild() returned an error: %v", err)
			return
		}
	}

	for i := int64(0); i != 3; i += 1 {
		docker[i].StartMonitor()
		chanList[i] = docker[i].GetChaosEvent()
	}

	event := mergeChannels(chanList...)

	testTimeout := time.NewTimer(60 * time.Minute)
	go func(testTimeout *time.Timer, docker *dockerBuilder.ContainerBuilder) {
		<-testTimeout.C
		var event = docker.GetChaosEvent()
		event <- dockerBuilder.Event{Done: true}
	}(testTimeout, docker[0])

	for {
		e := <-event

		if e.Error || e.Fail {
			fmt.Printf("container name: %v\n", e.ContainerName)
			log.Printf("Error: %v", e.Message)
			return
		}
		if e.Done || e.Error || e.Fail {

			fmt.Printf("container name: %v\n", e.ContainerName)
			fmt.Printf("done: %v\n", e.Done)
			fmt.Printf("fail: %v\n", e.Fail)
			fmt.Printf("error: %v\n", e.Error)

			break
		}
	}

	for i := int64(0); i != 3; i += 1 {
		_ = docker[i].StopMonitor()
	}

	return
}

func mergeChannels(cs ...<-chan dockerBuilder.Event) <-chan dockerBuilder.Event {
	out := make(chan dockerBuilder.Event)
	var wg sync.WaitGroup
	wg.Add(len(cs))
	for _, c := range cs {
		go func(c <-chan dockerBuilder.Event) {
			for v := range c {
				out <- v
			}
			wg.Done()
		}(c)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// dockerTestNetworkCreate
//
// English:
//
// Create a docker network for the simulations.
//
//	Output:
//	  netDocker: Pointer to the docker network manager object
//	  err: golang error
//
// Português:
//
// Cria uma rede docker para as simulações.
//
//	Saída:
//	  netDocker: Ponteiro para o objeto gerenciador de rede docker
//	  err: golang error
func dockerTestNetworkCreate() (
	netDocker *dockerBuilderNetwork.ContainerBuilderNetwork,
	err error,
) {

	// English: Create a network orchestrator for the container [optional]
	// Português: Cria um orquestrador de rede para o container [opcional]
	netDocker = &dockerBuilderNetwork.ContainerBuilderNetwork{}
	err = netDocker.Init()
	if err != nil {
		err = fmt.Errorf("dockerTestNetworkCreate().error: the function netDocker.Init() returned an error: %v", err)
		return
	}

	// English: Create a network named "cache_delete_after_test"
	// Português: Cria uma rede de nome "cache_delete_after_test"
	err = netDocker.NetworkCreate(
		"cache_delete_after_test",
		"10.0.0.0/16",
		"10.0.0.1",
	)
	if err != nil {
		err = fmt.Errorf("dockerTestNetworkCreate().error: the function netDocker.NetworkCreate() returned an error: %v", err)
		return
	}

	return
}

func dockerBarco(
	netDocker *dockerBuilderNetwork.ContainerBuilderNetwork,
	dockerContainer *dockerBuilder.ContainerBuilder,
	key int64,
) (
	err error,
) {

	test := &DockerfileGolang{}

	// English: set a docker network
	// Português: define a rede docker
	dockerContainer.SetNetworkDocker(netDocker)

	dockerContainer.SetDockerfileBuilder(test)

	err = dockerContainer.SetDockerfilePath("/internal/test/chaos/simpleRestart/Dockerfile")
	if err != nil {
		panic(err)
	}
	//dockerContainer.DockerfileAddCopyToFinalImage("./internal/test/chaos/simpleRestart/Dockerfile-iotmaker", "/Dockerfile-iotmaker")
	dockerContainer.SetCacheEnable(true)
	dockerContainer.SetImageCacheName("barco:latest")
	dockerContainer.SetImageExpirationTime(120 * time.Minute)

	dockerContainer.SetSceneNameOnChaosScene("barco")

	// English: define o nome da imagem a ser baixada e instalada.
	// Português: sets the name of the image to be downloaded and installed.
	dockerContainer.SetImageName("barco:latest")

	// English: defines the name of the Barco container to be created
	// Português: define o nome do container Barco a ser criado
	dockerContainer.SetContainerName("delete_barco_" + strconv.FormatInt(key, 10))

	if key == 0 {
		// English: sets the value of the container's network port and the host port to be exposed
		// Português: define o valor da porta de rede do container e da porta do hospedeiro a ser exposta
		dockerContainer.AddPortToChange("9250", "9250")
		dockerContainer.AddPortToChange("9251", "9251")
		dockerContainer.AddPortToChange("9252", "9252")
	}

	dockerContainer.SetPrintBuildOnStrOut()

	// English: sets Barco-specific environment variables (releases connections from any address)
	// Português: define variáveis de ambiente específicas do Barco (libera conexões de qualquer endereço)
	dockerContainer.SetEnvironmentVar(
		[]string{
			//"BARCO_DEV_MODE=true",
			"BARCO_SHUTDOWN_DELAY_SECS=0",
			"BARCO_CONSUMER_ADD_DELAY_MS=5000",
			"BARCO_SEGMENT_FLUSH_INTERVAL_MS=500",
			"BARCO_BROKER_NAMES=delete_barco_0,delete_barco_1,delete_barco_2",
			"BARCO_ORDINAL=" + strconv.FormatInt(key, 10),
		},
	)

	dockerContainer.SetBuildFolderPath("./")

	// English: defines a text to be searched for in the standard output of the container indicating the end of the installation
	// define um texto a ser procurado na saída padrão do container indicando o fim da instalação
	//dockerContainer.SetWaitStringWithTimeout(`"message":"Barco started"}`, 60*time.Second)
	dockerContainer.SetWaitStringWithTimeout(`"Starting Barco"`, 60*time.Second)

	// English: Defines the probability of the container restarting and changing the IP address in the process.
	//
	// Português: Define a probalidade do container reiniciar e mudar o endereço IP no processo.
	dockerContainer.SetRestartProbability(0.9, 1.0, 99999999)

	// English: Defines a time window used to start chaos testing after container initialized
	//
	// Português: Define uma janela de tempo usada para começar o teste de caos depois do container inicializado
	dockerContainer.SetTimeToStartChaosOnChaosScene(10*time.Second, 30*time.Second)

	// English: Sets a time window used to release container restart after the container has been initialized
	//
	// Português: Define uma janela de tempo usada para liberar o reinício do container depois do container ter sido inicializado
	dockerContainer.SetTimeBeforeStartChaosInThisContainerOnChaosScene(10*time.Second, 30*time.Second)

	// English: Defines a time window used to pause the container
	//
	// Português: Define uma janela de tempo usada para pausar o container
	dockerContainer.SetTimeOnContainerPausedStateOnChaosScene(10*time.Second, 30*time.Second)

	// English: Defines a time window used to unpause the container
	//
	// Português: Define uma janela de tempo usada para remover a pausa do container
	dockerContainer.SetTimeOnContainerUnpausedStateOnChaosScene(10*time.Second, 30*time.Second)

	// English: Sets a time window used to restart the container after stopping
	//
	// Português: Define uma janela de tempo usada para reiniciar o container depois de parado
	dockerContainer.SetTimeToRestartThisContainerAfterStopEventOnChaosScene(10*time.Second, 30*time.Second)

	dockerContainer.EnableChaosScene(true)

	// Adiciona um indicador de falha com gravação de arquivo em log ao projeto.
	// Indicador de falha é um texto procurado na saída padrão do container e indica algo que não deveria ter acontecido durante o teste.
	// Algumas falhas críticas podem ser monitoradas e quando elas acontecem, a saída padrão do container é arquivada em um arquivo `log.N.log`, onde N é um número incrementado automaticamente.
	err = dockerContainer.AddFailMatchFlagToFileLog(
		"\"level\":\"fatal\"",
		"./internal/test/chaos/simpleRestart/bug",
	)
	if err != nil {
		util.TraceToLog()
		log.Printf("Error: %v", err.Error())
		return
	}

	err = dockerContainer.AddFailMatchFlagToFileLog(
		"panic:",
		"./internal/test/chaos/simpleRestart/bug",
	)
	if err != nil {
		util.TraceToLog()
		log.Printf("Error: %v", err.Error())
		return
	}

	// English: initialize the docker control object
	// Português: inicializa o objeto de controle docker
	err = dockerContainer.Init()
	if err != nil {
		err = fmt.Errorf("dockerBarco.error: the function dockerContainer.Init() returned an error: %v", err)
		return
	}

	//err = dockerContainer.ImagePull()
	//if err != nil {
	//	err = fmt.Errorf("dockerBarco.error: the function dockerContainer.ImagePull() returned an error: %v", err)
	//	return
	//}

	_, err = dockerContainer.ImageBuildFromFolder()
	if err != nil {
		err = fmt.Errorf("dockerBarco.error: the function dockerContainer.ImageBuildFromFolder() returned an error: %v", err)
		return
	}

	// English: build a container
	// Português: monta o container
	err = dockerContainer.ContainerBuildWithoutStartingItFromImage()
	if err != nil {
		err = fmt.Errorf("dockerBarco.error: the function dockerContainer.ContainerBuildAndStartFromImage() returned an error: %v", err)
		return
	}

	return
}
