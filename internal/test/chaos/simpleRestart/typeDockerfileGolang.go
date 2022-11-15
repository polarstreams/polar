package simpleRestart

import (
	"github.com/docker/docker/api/types/mount"
	iotmakerdocker "github.com/helmutkemper/iotmaker.docker/v1.0.1"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	dockerfileGolang "github.com/helmutkemper/iotmaker.docker.builder.golang.dockerfile"
)

// ChangePort
//
// English:
//
//  Receives the relationship between ports to be exchanged
//
//   oldPort: porta original da imagem
//   newPort: porta a exporta na rede
//
// Português:
//
//  Recebe a relação entre portas a serem trocadas
//
//   oldPort: porta original da imagem
//   newPort: porta a exporta na rede

type Copy struct {
	Src string
	Dst string
}

type DockerfileGolang struct {
	disableScratch     bool
	finalImageName     string
	sshDefaultFileName string
	copy               []Copy
}

// SetFinalImageName
//
// English:
//
//	Set a two stage build final image name
//
// Português:
//
//	Define o nome da imagem final para construção de imagem em dois estágios.
func (e *DockerfileGolang) SetFinalImageName(name string) {
	e.DisableScratch()
	e.finalImageName = name
}

// DisableScratch
//
// English:
//
//	Change final docker image from scratch to image name set in SetFinalImageName() function
//
// Português:
//
//	Troca a imagem final do docker de scratch para o nome da imagem definida na função
//	SetFinalImageName()
func (e *DockerfileGolang) DisableScratch() {
	e.disableScratch = true
}

// AddCopyToFinalImage
//
// English:
//
//	Add one instruction 'COPY --from=builder /app/`dst` `src`' to final image builder.
//
// Português:
//
//	Adiciona uma instrução 'COPY --from=builder /app/`dst` `src`' ao builder da imagem final.
func (e *DockerfileGolang) AddCopyToFinalImage(src, dst string) {
	if e.copy == nil {
		e.copy = make([]Copy, 0)
	}

	e.copy = append(e.copy, Copy{Src: src, Dst: dst})
}

// SetDefaultSshFileName
//
// English:
//
//	Sets the name of the file used as the ssh key.
//
// Português:
//
//	Define o nome do arquivo usado como chave ssh.
func (e *DockerfileGolang) SetDefaultSshFileName(name string) {
	e.sshDefaultFileName = name
}

// Prayer
//
// English:
//
//	Programmer prayer.
//
// Português:
//
//	Oração do programador.
func (e *DockerfileGolang) Prayer() {
	log.Print("Português:")
	log.Print("Código nosso que estais em Golang\nSantificado seja Vós, Console\nVenha a nós a Vossa Reflexão\nE seja feita as {Vossas chaves}\nAssim no if(){}\nComo no else{}\nO for (nosso; de cada dia; nos dai hoje++)\nDebugai as nossas sentenças\nAssim como nós colocamos\nO ponto e vírgula esquecido;\nE não nos\n\tdeixeis errar\n\t\ta indentação\nMas, livrai-nos das funções recursivas\nA main()")
	log.Print("")
}

// MountDefaultDockerfile
//
// English:
//
//	Build a default dockerfile for image
//
//	 Input:
//	   args: list of environment variables used in the container
//	   changePorts: list of ports to be exposed on the network, used in the original image that should be changed. E.g.: 27017 to 27016
//	   openPorts: list of ports to be exposed on the network
//	   exposePorts: list of ports to just be added to the project's dockerfile
//	   volumes: list of folders and files with permission to share between the container and the host.
//
//	 Output:
//	   dockerfile: string containing the project's dockerfile
//	   err: standard error object
//
// Português:
//
//	Monta o dockerfile padrão para a imagem
//
//	 Entrada:
//	   args: lista de variáveis de ambiente usadas no container
//	   changePorts: lista de portas a serem expostas na rede, usadas na imagem original que devem ser trocadas. Ex.: 27017 para 27016
//	   openPorts: lista de portas a serem expostas na rede
//	   exposePorts: lista de portas a serem apenas adicionadas ao dockerfile do projeto
//	   volumes: lista de pastas e arquivos com permissão de compartilhamento entre o container e o hospedeiro.
//
//	 Saída:
//	   dockerfile: string contendo o dockerfile do projeto
//	   err: objeto de erro padrão
func (e *DockerfileGolang) MountDefaultDockerfile(
	args map[string]*string,
	changePorts []dockerfileGolang.ChangePort,
	openPorts []string,
	exposePorts []string,
	volumes []mount.Mount,
	installExtraPackages bool,
	useCache bool,
	imageCacheName string,
) (
	dockerfile string,
	err error,
) {

	var info fs.FileInfo
	var found bool

	if e.finalImageName == "" {
		e.finalImageName = "golang:1.19-alpine"
	}

	if e.sshDefaultFileName == "" {
		e.sshDefaultFileName = "id_ecdsa"
	}

	if useCache == true {
		dockerfile += `
# (en) first stage of the process
# (pt) primeira etapa do processo
FROM ` + imageCacheName + ` as builder
#
`
	} else {
		dockerfile += `
# (en) first stage of the process
# (pt) primeira etapa do processo
FROM ` + e.finalImageName + ` as builder
#
`
	}

	for k := range args {
		switch k {
		case "SSH_ID_RSA_FILE":
			dockerfile += `
# (en) content from file /root/.ssh/id_rsa
# (pt) conteúdo do arquivo /root/.ssh/id_rsa
ARG SSH_ID_RSA_FILE
`
		case "KNOWN_HOSTS_FILE":
			dockerfile += `
# (en) content from file /root/.ssh/know_hosts
# (pt) conteúdo do arquivo /root/.ssh/know_hosts
ARG KNOWN_HOSTS_FILE
`
		case "GITCONFIG_FILE":
			dockerfile += `
# (en) content from file /root/.gitconfig
# (pt) conteúdo do arquivo /root/.gitconfig
ARG GITCONFIG_FILE
`
		case "GIT_PRIVATE_REPO":
			dockerfile += `
# (en) path from private repository. example: github.com/helmutkemper
# (pt) caminho do repositório privado. exemplo: github.com/helmutkemper
ARG GIT_PRIVATE_REPO
`
		default:
			dockerfile += `
ARG ` + k + `
`
		}
	}

	if installExtraPackages == true {
		dockerfile += `
#
# (en) Add open ssl to alpine
# (pr) Adiciona o open ssl ao apine
RUN apk add openssh && \
    # (en) creates the .ssh directory within the root directory
    # (pt) cria o diretório .ssh dentro do diretório root
    mkdir -p /root/.ssh/ && \
`
	} else {
		dockerfile += `
# (en) creates the .ssh directory within the root directory
# (pt) cria o diretório .ssh dentro do diretório root
RUN mkdir -p /root/.ssh/ && \
`
	}

	_, found = args["SSH_ID_RSA_FILE"]
	if found == true {
		dockerfile += `
    # (en) creates the id_esa file inside the .ssh directory
    # (pt) cria o arquivo id_esa dentro do diretório .ssh
    echo "$SSH_ID_RSA_FILE" > /root/.ssh/` + e.sshDefaultFileName + ` && \
    # (en) adjust file access security
    # (pt) ajusta a segurança de acesso do arquivo
    chmod -R 600 /root/.ssh/ && \
`
	}

	_, found = args["SSH_ID_ECDSA_FILE"]
	if found == true {
		dockerfile += `
    # (en) creates the id_ecdsa file inside the .ssh directory
    # (pt) cria o arquivo id_ecdsa dentro do diretório .ssh
    echo "SSH_ID_ECDSA_FILE" > /root/.ssh/id_ecdsa && \
    # (en) adjust file access security
    # (pt) ajusta a segurança de acesso do arquivo
    chmod -R 600 /root/.ssh/ && \
`
	}

	_, found = args["KNOWN_HOSTS_FILE"]
	if found == true {
		dockerfile += `
    # (en) creates the known_hosts file inside the .ssh directory
    # (pt) cria o arquivo known_hosts dentro do diretório .ssh
    echo "$KNOWN_HOSTS_FILE" > /root/.ssh/known_hosts && \
    # (en) adjust file access security
    # (pt) ajusta a segurança de acesso do arquivo
    chmod -R 600 /root/.ssh/known_hosts && \
`
	}

	_, found = args["GITCONFIG_FILE"]
	if found == true {
		dockerfile += `
    # (en) creates the .gitconfig file at the root of the root directory
    # (pt) cria o arquivo .gitconfig na raiz do diretório /root
    echo "$GITCONFIG_FILE" > /root/.gitconfig && \
    # (en) adjust file access security
    # (pt) ajusta a segurança de acesso do arquivo
    chmod -R 600 /root/.gitconfig && \
`
	}

	if installExtraPackages == true {
		dockerfile += `
    # (en) prepares the OS for installation
    # (pt) prepara o OS para instalação
    apk update && \
    # (en) install binutils, file, gcc, g++, make, libc-dev, fortify-headers and patch
    # (pt) instala binutils, file, gcc, g++, make, libc-dev, fortify-headers e patch
    apk add --no-cache build-base && \
    # (en) install git, fakeroot, scanelf, openssl, apk-tools, libc-utils, attr, tar, pkgconf, patch, lzip, curl,
    #      /bin/sh, so:libc.musl-x86_64.so.1, so:libcrypto.so.1.1 and so:libz.so.1
    # (pt) instala git, fakeroot, scanelf, openssl, apk-tools, libc-utils, attr, tar, pkgconf, patch, lzip, curl,
    #      /bin/sh, so:libc.musl-x86_64.so.1, so:libcrypto.so.1.1 e so:libz.so.1
    apk add --no-cache alpine-sdk && \
`
	}

	dockerfile += `
    # (en) clear the cache
    # (pt) limpa a cache
    rm -rf /var/cache/apk/*
#
# (en) creates the /app directory, where your code will be installed
# (pt) cria o diretório /app, onde seu código vai ser instalado
WORKDIR /app
# (en) copy your project into the /app folder
# (pt) copia seu projeto para dentro da pasta /app
COPY . .
# (en) enables the golang compiler to run on an extremely simple OS, scratch
# (pt) habilita o compilador do golang para rodar em um OS extremamente simples, o scratch
ARG CGO_ENABLED=1
# (en) adjust git to work with shh
# (pt) ajusta o git para funcionar com shh

RUN git config --global url.ssh://git@github.com/.insteadOf https://github.com/
`

	_, found = args["GIT_PRIVATE_REPO"]
	if found == true {
		dockerfile += `
# (en) defines the path of the private repository
# (pt) define o caminho do repositório privado
RUN go env -w GOPRIVATE=$GIT_PRIVATE_REPO
`
	}

	dockerfile += `
# (en) install the dependencies in the go.mod file
# (pt) instala as dependências no arquivo go.mod
RUN go mod tidy
# (en) compiles the main.go file
# (pt) compila o arquivo main.go
RUN go build -ldflags="-w -s" -o /app/main /app/main.go
# (en) creates a new scratch-based image
# (pt) cria uma nova imagem baseada no scratch
# (en) scratch is an extremely simple OS capable of generating very small images
# (pt) o scratch é um OS extremamente simples capaz de gerar imagens muito reduzidas
# (en) discarding the previous image erases git access credentials for your security and reduces the size of the
#      image to save server space
# (pt) descartar a imagem anterior apaga as credenciais de acesso ao git para a sua segurança e reduz o tamanho
#      da imagem para poupar espaço no servidor
`
	if e.disableScratch == true {
		dockerfile += `
FROM ` + e.finalImageName + `
# (en) copy your project to the new image
# (pt) copia o seu projeto para a nova imagem
COPY --from=builder /app/main .
`

		for _, copyFile := range e.copy {
			dockerfile += `COPY --from=builder /app/` + copyFile.Dst + ` ` + copyFile.Src + `
`
		}

		dockerfile += `# (en) execute your project
# (pt) executa o seu projeto
`
	} else {
		dockerfile += `
FROM scratch
# (en) copy your project to the new image
# (pt) copia o seu projeto para a nova imagem
COPY --from=builder /app/main .
`

		for _, copyFile := range e.copy {
			dockerfile += `COPY --from=builder /app/` + copyFile.Dst + ` ` + copyFile.Src + `
`
		}

		dockerfile += `# (en) execute your project
# (pt) executa o seu projeto
`
	}
	var exposeList = make([]string, 0)
	for _, v := range changePorts {
		var pass = true
		for _, expose := range exposeList {
			if expose == v.OldPort {
				pass = false
				break
			}
		}

		if pass == false {
			continue
		}
		exposeList = append(exposeList, v.OldPort)

		dockerfile += `EXPOSE ` + v.OldPort + `
`
	}

	for _, v := range openPorts {
		var pass = true
		for _, expose := range exposeList {
			if expose == v {
				pass = false
				break
			}
		}

		if pass == false {
			continue
		}
		exposeList = append(exposeList, v)

		dockerfile += `EXPOSE ` + v + `
`
	}

	for _, v := range exposePorts {
		var pass = true
		for _, expose := range exposeList {
			if expose == v {
				pass = false
				break
			}
		}

		if pass == false {
			continue
		}
		exposeList = append(exposeList, v)

		dockerfile += `EXPOSE ` + v + `
`
	}

	var volumeList = make([]string, 0)
	for _, v := range volumes {

		var newPath string
		if v.Type != iotmakerdocker.KVolumeMountTypeBindString {
			continue
		}
		info, err = os.Stat(v.Source)
		if err != nil {
			return
		}
		if info.IsDir() == true {
			newPath = v.Target
		} else {
			var dir string
			dir, _ = filepath.Split(v.Target)
			newPath = dir
		}

		if strings.HasSuffix(newPath, "/") == true {
			newPath = strings.TrimSuffix(newPath, "/")
		}

		var pass = false
		for _, volume := range volumeList {
			if volume == newPath {
				pass = true
				break
			}
		}

		if pass == false {
			dockerfile += `VOLUME ` + newPath + `
`
			volumeList = append(volumeList, newPath)
		}
	}

	dockerfile += `
CMD ["/main"]
`

	strings.ReplaceAll(dockerfile, "\r", "")
	var lineList = strings.Split(dockerfile, "\n")
	dockerfile = ""
	for _, line := range lineList {
		if strings.Trim(line, " ") != "" {
			dockerfile += line + "\r\n"
		}
	}

	return
}
