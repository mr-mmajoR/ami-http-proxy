package main

import (
	//"encoding/json"
	"bufio"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/olebedev/config"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type (
	// map of values for commands and results;
	TValues map[string]string

	TEventQueue []*TValues

	TCommand struct {
		Action string
		Values TValues
	}

	// Queue of  commands
	TCommands chan TCommand

	TClient struct {
		Name         string
		token        string
		lastActivity time.Time

		sync.RWMutex
		eventsQueue TEventQueue
	}

	TClientPool struct {
		sync.RWMutex
		items []*TClient
	}

	// Queue of results contet clients with their events
	TResults chan *TClient
)

var (
	l   *log.Logger
	cfg *config.Config

	ami_host     string
	ami_port     string
	ami_username string
	ami_password string
	ami_conn     net.Conn

	bind_port      string
	bind_interface string

	ami_commands TCommands
	ami_results  TResults

	clientPool TClientPool
)

const (
	logFileName    = "logger.log"
	configFileName = "config.json"
	nl             = "\r\n"
)

// convert action with values to string
func makeAmiCommand(Action string, values TValues) string {
	cmd := "Action:" + Action + nl
	for i, v := range values {
		cmd += i + ":" + v + nl
	}
	cmd += nl
	return cmd
}

//  send command to AMI
func sendAmiCommand(cmd TCommand) (err error) {
	l.Println("sendAmiCommand ", cmd)
	ami_conn.Write([]byte(makeAmiCommand(cmd.Action, cmd.Values)))
	return
}

// open connection to AMI
func openAmiConnection(host, port, username, password string) (conn *net.TCPConn, err error) {
	servAddr := host + ":" + port
	tcpAddr, err := net.ResolveTCPAddr("tcp", servAddr)
	if err != nil {
		l.Fatalln("ResolveTCPAddr failed:", err.Error())
	}
	conn, err = net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		l.Fatalln("Dial failed:", err.Error())
	}
	r := bufio.NewReader(conn)
	line, _ := r.ReadString('\n')
	if err != nil {
		return nil, errors.New("First line read error")
	}
	l.Println("Connected:", line)
	cmd := makeAmiCommand("Login", TValues{"Username": ami_username, "Secret": ami_password})
	//fmt.Println(cmd)
	conn.Write([]byte(cmd))
	line, err = r.ReadString('\n')
	if err != nil {
		return nil, errors.New("Authentification error")
	}
	if strings.TrimSpace(line) != "Response: Success" {
		return nil, errors.New("Authentification error - " + line)
	}
	line, err = r.ReadString('\n')
	line, err = r.ReadString('\n')
	if err != nil && line == "" {
		return nil, errors.New("Authentification error")
	}

	return conn, err
}

func init() {

	f, err := os.OpenFile(logFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Failed to open log file ", logFileName, ":", err)
	}

	multi := io.MultiWriter(f, os.Stdout)
	l = log.New(multi, "main: ", log.Ldate|log.Ltime|log.Lshortfile)

	cfg, err := config.ParseJsonFile(configFileName)
	if err != nil {
		l.Fatalln("Failed to parse config file ", configFileName, ":", err)
	}

	ami_host, _ = cfg.String("ami.host")
	ami_port, _ = cfg.String("ami.port")
	ami_username, _ = cfg.String("ami.username")
	ami_password, _ = cfg.String("ami.password")

	bind_port, _ = cfg.String("main.port")
	bind_interface, _ = cfg.String("main.interface")

	ami_commands = make(TCommands)
	ami_results = make(TResults)

	clientPool = TClientPool{items: []*TClient{}}

}

// http handler for 1C users registartion
func register_handler(w http.ResponseWriter, r *http.Request) {

	r.ParseForm()
	token := r.FormValue("token")

	var c *TClient

	c = clientPool.FindClient(token)
	if c == nil {
		c = NewClient(strings.TrimSpace(token))
	}
	c.Name = r.FormValue("name")

	clientPool.AddClient(c)
	l.Printf("Client register: %s token=%s \r\n", c.Name, c.token)

	io.WriteString(w, c.token)

	return
}

// http handler for proxy Actions and Respond result + events
func api_handler(w http.ResponseWriter, r *http.Request) {

	r.ParseForm()
	//fmt.Println(r.Form)

	token := r.FormValue("token")

	cmd := TCommand{Action: r.FormValue("Action"), Values: TValues{}}
	for k, v := range r.Form {
		if k == "Action" || k == "token" {
			continue
		}
		cmd.Values[k] = strings.TrimSpace(v[0])
	}
	cmd.Values["ActionID"] = token

	//l.Println("api_handler: отправляю ", cmd)
	ami_commands <- cmd

	c := <-ami_results
	//l.Println("api_handler: принял ", *c)

	sres, err := c.GetEventsAsString()
	if err != nil {
		l.Println("api_handler error", cmd, c)
		l.Println(err)
	}
	c.ClearEvents()
	c.UpdateActivity()

	io.WriteString(w, sres)
}

func main() {
	var err error
	ami_conn, err = openAmiConnection(ami_host, ami_port, ami_username, ami_password)
	if err != nil {
		l.Fatalln("Error open connection to AMI ", err)
	}

	// commands processsor
	go func() {
		for cmd := range ami_commands {
			if cmd.Action == "STOP" {
				break
			}
			err := sendAmiCommand(cmd)
			if err != nil {
				l.Fatalln("Error send command to AMI ", err)
				break
			}

			time.Sleep(500 * time.Millisecond)
			ami_results <- clientPool.FindClient(cmd.Values["ActionID"])
		}
	}()

	// evet processor
	go func() {
		r := bufio.NewReader(ami_conn)
		data := make(TValues)
		for {
			line, err := r.ReadString('\n')
			//fmt.Println("DEBUG READ:", line)
			if err != nil {
				time.Sleep(10 * time.Millisecond)
				fmt.Print(".")
			} else if strings.TrimSpace(line) == "" {
				fmt.Println("EVENT: ", data)
				clientPool.AddEventResult(data)
				data = make(TValues)
			} else {
				kv := strings.SplitN(line, ":", 2)
				data[kv[0]] = strings.TrimSpace(kv[1])
			}
		}
	}()

	// clear clients with old activity
	go func() {
		for {
			time.Sleep(2 * time.Minute)
			clientPool.RemoveNotActive()
		}
	}()

	fmt.Println()
	fmt.Println("----------------------------------------------------------------------------------------")
	fmt.Println("| 1C AMI ROUTER IS STARTED  at ", bind_interface+":"+bind_port)
	fmt.Println("| /register/?token=<token>&name=<name>  - 1C registration")
	fmt.Println("| /api/?token=<token>&Action=<action>&Value1=<value1>&Value2=<value2> - 1C action route")
	fmt.Println("----------------------------------------------------------------------------------------")

	rtr := mux.NewRouter()
	rtr.HandleFunc("/register/", register_handler).Methods("GET")
	rtr.HandleFunc("/api/", api_handler).Methods("GET")
	http.Handle("/", rtr)
	http.ListenAndServe(bind_interface+":"+bind_port, nil) // set listen port

}

// TClients -----------------------------------------------------------------
func NewClient(atoken string) *TClient {
	if atoken == "" {
		atoken = fmt.Sprintf("%v", time.Now().UnixNano())
	}
	atoken = strings.TrimSpace(atoken)
	return &TClient{token: atoken, Name: fmt.Sprintf("%s", atoken), lastActivity: time.Now(), eventsQueue: TEventQueue{}}
}

func (c *TClient) AddEvent(v *TValues) {
	c.Lock()
	defer c.Unlock()
	c.eventsQueue = append(c.eventsQueue, v)
	//l.Println("AddEvent: Клиенту:", c.Name, " добавлено событие ", *v)
}

func (c *TClient) ClearEvents() {

	c.Lock()
	defer c.Unlock()

	c.eventsQueue = TEventQueue{}
	//println("ClearEvents: У клиента:", c.Name, " очищены все события")
}

func (c *TClient) GetEventsAsString() (sres string, err error) {

	c.RLock()
	defer c.RUnlock()

	for _, e := range c.eventsQueue {
		if e != nil {
			for k, v := range *e {
				sres += k + ":" + v + nl
			}
			sres += nl
		} else {
			err = errors.New("saddenly nil happend")
		}
	}
	sres += nl
	return
}

func (c *TClient) UpdateActivity() {
	c.lastActivity = time.Now()
	return
}

// TClientPool ------------------------------------------------------------
func (cp *TClientPool) FindClient(token string) *TClient {
	var res *TClient
	cp.RLock()
	defer cp.RUnlock()

	for _, c := range cp.items {
		if c.token == strings.TrimSpace(token) {
			res = c
		}
	}
	return res
}

func (cp *TClientPool) AddClient(c *TClient) {
	cp.Lock()
	defer cp.Unlock()
	cp.items = append(cp.items, c)
}

func (cp *TClientPool) AddEventResult(v TValues) {

	if v["ActionID"] != "" {
		token := v["ActionID"]
		c := cp.FindClient(token)
		if c != nil {
			c.AddEvent(&v)
		} else {
			l.Println("AddEventResult: not found client with token ", token)
		}
	} else {
		for _, c := range cp.items {
			c.AddEvent(&v)
		}
	}

}

func (cp *TClientPool) RemoveNotActive() {

	cp.Lock()
	defer cp.Unlock()

	n := time.Now()
	new_items := []*TClient{}
	deleted := false

	for _, c := range cp.items {
		if n.Sub(c.lastActivity) > 5*time.Minute {
			deleted = true
			l.Println("Deleted client ", c.Name, c.token)
		} else {
			new_items = append(new_items, c)
		}
	}
	if deleted {
		cp.items = new_items
	}
}
