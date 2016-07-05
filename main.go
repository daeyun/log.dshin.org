package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo"
	"github.com/labstack/echo/engine/standard"
	"github.com/labstack/echo/middleware"
	"github.com/satori/go.uuid"
	"gopkg.in/pg.v4"
	"hash/fnv"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Global variables.
var (
	authConfig struct {
		accessKey        string
		jwtSecretKey     string
		jwtSigningMethod jwt.SigningMethod
	}
	activeStreams = struct {
		sync.RWMutex
		streams map[string]*Stream
	}{streams: make(map[string]*Stream)}
	upgrader = websocket.Upgrader{}
	prng     = rand.NewSource(time.Now().UnixNano())
	db       *pg.DB
)

const kStreamBufferSize = 10
const kMaxMessageSize = 4096

const kPingTimeout = time.Second * 20
const kPingInterval = time.Minute * 2

type Stream struct {
	Id string `json:"streamId"`
	// Not exported.
	channel chan QueryResults
	queries []Query
}

type Query struct {
	Id                 string `json:"queryId"`
	MessageId          int64  `json:"messageId"`
	MessageIdNewerThan int64  `json:"messageIdNewerThan"`
	NameLike           string `json:"nameLike"`
	BodyRegex          string `json:"bodyRegex"`
	TargetStreamId     string `json:"targetStreamId"`
}

type QueryResults struct {
	Messages []Message `json:"results"`
	QueryIds []string  `json:"queryIds"`
}

// Also used as DB schema.
type Message struct {
	Id       int64  `json:"messageId"`
	Time     int64  `json:"time"`
	Name     string `json:"name"`
	Body     string `json:"body"`
	Metadata string `json:"metadata"`
}

func (m Message) String() string {
	return fmt.Sprintf("Message<%d %d [%s]: %s>", m.Id, m.Time, m.Name, m.Body)
}

// Request and Response models.
type AuthRequest struct {
	AccessKey string `json:"accessKey"`
}

type AuthResponse struct {
	AccessToken string `json:"accessToken"`
	ExpiresIn   int    `json:"expiresIn"`
}

type PingMessage struct {
	Ping int64 `json:"ping"`
}

type PongMessage struct {
	Pong int64 `json:"pong"`
}

// Request handlers.
func authRequestHandler(c echo.Context) error {
	authRequest := new(AuthRequest)
	if err := c.Bind(authRequest); err != nil {
		return err
	}
	log.Println("Key", authRequest.AccessKey)
	if authRequest.AccessKey == authConfig.accessKey {
		// Create token.
		token := jwt.New(authConfig.jwtSigningMethod)

		// Set claims.
		claims := token.Claims.(jwt.MapClaims)
		expiresIn := time.Hour * 24
		claims["exp"] = time.Now().Add(expiresIn).Unix()

		// Generate encoded token and send it as response.
		signedToken, err := token.SignedString([]byte(authConfig.jwtSecretKey))
		if err != nil {
			return err
		}
		return c.JSON(http.StatusOK, AuthResponse{
			signedToken,
			int(expiresIn.Seconds()),
		})
	}

	return echo.ErrUnauthorized
}

func saveToDatabase(message Message, db *pg.DB) (int64, error) {
	err := db.Create(&message)
	return message.Id, err
}

func queryDatabase(query Query, db *pg.DB) (*[]Message, error) {
	var msgs []Message
	sqlQuery := db.Model(&msgs)
	if query.MessageIdNewerThan != 0 && query.MessageId != 0 {
		return &msgs, errors.New("Only one of messageIdNewerThan and messageId should be set.")
	}
	if query.MessageId != 0 {
		sqlQuery = sqlQuery.Where("id = ?", query.MessageId)
		//log.Println("Applying WHERE id =", query.MessageId)
	} else if query.MessageIdNewerThan != 0 {
		sqlQuery = sqlQuery.Where("id > ?", query.MessageIdNewerThan)
		//log.Println("Applying WHERE id >", query.messageIdNewerThan)
	}
	if len(query.NameLike) > 0 {
		sqlQuery = sqlQuery.Where("name LIKE ?", query.NameLike)
		//log.Println("Applying name like", query.NameLike)
	}
	if len(query.BodyRegex) > 0 {
		sqlQuery = sqlQuery.Where("body ~ ?", query.BodyRegex)
		//log.Println("Applying body similar to", query.BodyRegex)
	}
	err := sqlQuery.Select()
	return &msgs, err
}

func newMessageHandler(c echo.Context) error {
	message := new(Message)
	if err := c.Bind(message); err != nil {
		log.Println(err)
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "bad request"})
	}
	if len(message.Body) == 0 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "body should not be empty"})
	}
	if len(message.Name) == 0 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "name should not be empty"})
	}
	if message.Id != 0 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "id should be empty"})
	}
	if message.Time != 0 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "time should be empty"})
	}
	receivedTime := time.Now()
	message.Time = receivedTime.UnixNano()

	// Save the incoming message to database.
	id, err := saveToDatabase(*message, db)
	if err != nil {
		log.Println(err)
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "could not save to database. see logs."})
	}
	message.Id = id

	// Then loop through the active queries and route to any matching streams asynchronously.
	go func() {
		activeStreams.RLock()
		defer activeStreams.RUnlock()
		for _, v := range activeStreams.streams {
			queryIds := make([]string, 0)
			for i := range v.queries {
				query := v.queries[i]
				query.MessageId = message.Id
				// TODO(daeyun): This is too slow.
				result, err := queryDatabase(query, db)
				//log.Println("Results found: ", result)
				if err == nil && len(*result) != 0 {
					//log.Println("appended")
					queryIds = append(queryIds, query.Id)
				} else if err != nil {
					// TODO(daeyun): This should not happen if the queries are
					// already validated. Just log for now.
					log.Println(err)
				}
			}
			if len(queryIds) > 0 {
				v.channel <- QueryResults{
					QueryIds: queryIds,
					Messages: []Message{*message},
				}
			}
		}
	}()

	return c.JSON(http.StatusOK, map[string]int64{
		// Microseconds.
		"messageId": message.Id,
		"elapsed":   int64(time.Since(receivedTime).Nanoseconds() / 1000),
	})
}

func querySubmissionHandler(c echo.Context) error {
	query := new(Query)
	if err := c.Bind(query); err != nil {
		log.Println(err)
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "bad request"})
	}
	if len(query.TargetStreamId) == 0 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "targetStreamId should not be empty"})
	}
	if len(query.NameLike) == 0 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "nameLike should not be empty"})
	}
	if len(query.BodyRegex) == 0 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "bodyRegex should not be empty"})
	}
	if len(query.Id) != 0 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Id should be empty"})
	}
	if query.MessageIdNewerThan < 0 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "messageIdNewerThan should not be a negative number"})
	}
	// TODO(daeyun): Check if regex is valid. Also check for duplicates.
	hash := fnv.New64a()
	str, _ := json.Marshal(query)
	hash.Write(str)
	query.Id = strconv.FormatUint(hash.Sum64(), 36) // [0-9a-z]+
	startTime := time.Now()

	activeStreams.Lock()
	defer activeStreams.Unlock()
	if stream, ok := activeStreams.streams[query.TargetStreamId]; ok {
		stream.queries = append(stream.queries, *query)
		// This probably takes a while.
		messages, err := queryDatabase(*query, db)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}
		stream.channel <- QueryResults{
			Messages: *messages,
			QueryIds: []string{query.Id},
		}
		return c.JSON(http.StatusOK, map[string]interface{}{
			"queryId": query.Id,
			"elapsed": time.Since(startTime).Nanoseconds() / 1000,
		})
	}
	return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid stream id"})
}

func queryRemovalHandler(c echo.Context) error {
	query := new(Query)
	if err := c.Bind(query); err != nil {
		log.Println(err)
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "bad request"})
	}
	if len(query.Id) == 0 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "queryId should not be empty"})
	}

	startTime := time.Now()
	activeStreams.Lock()
	defer activeStreams.Unlock()
	for _, v := range activeStreams.streams {
		// Delete "in-place" while iterating backwards.
		for i := len(v.queries) - 1; i >= 0; i-- {
			if v.queries[i].Id == query.Id {
				v.queries = append(v.queries[:i], v.queries[i+1:]...)
			}
		}
	}
	return c.JSON(http.StatusOK, map[string]int64{
		"elapsed": time.Since(startTime).Nanoseconds() / 1000,
	})
}

func authenticateAndUpgradeWebsocket(w http.ResponseWriter, r *http.Request) *websocket.Conn {
	cookie, err := r.Cookie("accessToken")
	if err != nil {
		w.WriteHeader(http.StatusUnauthorized)
		log.Println(err)
		return nil
	}
	//cookie.Secure
	clientAccessToken := cookie.Value
	token, err := jwt.Parse(clientAccessToken, func(t *jwt.Token) (interface{}, error) {
		// Check the signing method
		if t.Method.Alg() != authConfig.jwtSigningMethod.Alg() {
			return nil, fmt.Errorf("unexpected JWT signing method=%v", t.Header["alg"])
		}
		return []byte(authConfig.jwtSecretKey), nil
	})
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return nil
	}
	if !token.Valid {
		w.WriteHeader(http.StatusUnauthorized)
		log.Println("Unauthorized")
		return nil
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println("Upgrade error:", err)
		return nil
	}
	return ws
}

func streamConnectionHandler(w http.ResponseWriter, r *http.Request) {
	if ws := authenticateAndUpgradeWebsocket(w, r); ws != nil {
		channel := make(chan QueryResults, kStreamBufferSize)

		stream := Stream{
			Id:      uuid.NewV4().String(),
			channel: channel,
			queries: []Query{},
		}

		activeStreams.Lock()
		activeStreams.streams[stream.Id] = &stream
		activeStreams.Unlock()

		defer func() {
			if err := ws.Close(); err != nil {
				log.Println("Close() error: ", err)
			}
			activeStreams.Lock()
			delete(activeStreams.streams, stream.Id)
			activeStreams.Unlock()
		}()

		// Send stream ID.
		websocket.WriteJSON(ws, stream)

		ws.SetReadLimit(kMaxMessageSize)
		ws.SetReadDeadline(time.Now().Add(kPingTimeout + kPingInterval))

		connectionClosed := make(chan bool)
		pongChannel := make(chan PongMessage)
		var waitGroup sync.WaitGroup
		waitGroup.Add(2)

		go func() {
			ticker := time.NewTicker(kPingInterval)
			defer ticker.Stop()
		PingLoop:
			for {
				pingMessage := PingMessage{time.Now().Unix()}
				select {
				case <-ticker.C:
					pingMessage.Ping = time.Now().Unix()
					if err := ws.WriteJSON(pingMessage); err != nil {
						log.Println("WriteJSON(pingMessage) failed:", err)
						break PingLoop
					}
				case pongMessage := <-pongChannel:
					if pongMessage.Pong == pingMessage.Ping {
						ws.SetReadDeadline(time.Now().Add(kPingTimeout + kPingInterval))
					} else if pongMessage.Pong == 0 {
						// Channel closed.
						break PingLoop
					}
				}
			}
			log.Println("Ping timeout or client closed connection.")
			waitGroup.Done()
		}()

		go func() {
		ReadLoop:
			for {
				for {
					pongMessage := PongMessage{}
					if err := ws.ReadJSON(&pongMessage); err != nil {
						log.Println("ReadJSON failed:", err)
						break ReadLoop
					} else if pongMessage.Pong != 0 {
						pongChannel <- pongMessage
					} else {
						// Ignore other messages.
						log.Println("Message from client:", pongMessage)
					}
				}
				time.Sleep(kPingInterval)
			}
			close(pongChannel)
			connectionClosed <- true
			waitGroup.Done()
		}()

	WriteLoop:
		for {
			select {
			case queryResults := <-channel:
				if err := ws.WriteJSON(queryResults); err != nil {
					log.Println("WriteJSON failed:", err)
					break WriteLoop
				}
			case <-connectionClosed:
				break WriteLoop
			}
		}

		waitGroup.Wait()
	}

	log.Println("END")
}

func createSchema(db *pg.DB) error {
	queries := []string{
		`CREATE TABLE messages (
    id bigserial PRIMARY KEY,
    time bigint NOT NULL,
    name varchar(64) NOT NULL,
    body text NOT NULL,
    metadata text
)`,
	}
	for _, q := range queries {
		_, err := db.Exec(q)
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	var port *int = flag.Int("port", 8080, "Port")
	var accessKey *string = flag.String("accessKey", "123", "Key used by clients to access data.")
	var jwtSecretKey *string = flag.String("jwtSecretKey", "42", "Key used for signing auth tokens.")
	var dbAddr *string = flag.String("dbAddr", "localhost:5432", "DB host:port")
	var dbUser *string = flag.String("dbUser", "postgres", "DB user")
	var dbName *string = flag.String("dbName", "postgres", "DB name")
	flag.Parse()

	authConfig.accessKey = *accessKey
	authConfig.jwtSecretKey = *jwtSecretKey
	authConfig.jwtSigningMethod = jwt.SigningMethodHS384

	db = pg.Connect(&pg.Options{
		Network:  "tcp",
		Addr:     *dbAddr,
		User:     *dbUser,
		Password: "",
		Database: *dbName,
	})
	if err := createSchema(db); err != nil {
		// If it already exists, OK.
		log.Println(err)
	}

	server := echo.New()

	// Middleware
	server.Use(middleware.Logger())
	server.Use(middleware.Recover())
	//server.Use(middleware.Secure())
	server.Use(middleware.BodyLimit("4KB")) // Maximum size for request body.

	server.Static("/static", "static")

	// Route => handler
	server.POST("/login", authRequestHandler)
	server.GET("/log/stream", standard.WrapHandler(http.HandlerFunc(streamConnectionHandler)))

	jwt := middleware.JWTWithConfig(middleware.JWTConfig{
		SigningKey:    []byte(authConfig.jwtSecretKey),
		SigningMethod: authConfig.jwtSigningMethod.Alg(),
	})
	server.POST("/log/new", newMessageHandler, jwt)
	server.POST("/log/subscribe", querySubmissionHandler, jwt)
	server.POST("/log/unsubscribe", queryRemovalHandler, jwt)
	server.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	}, jwt)

	// Start server
	addr := fmt.Sprintf(":%d", *port)
	server.Run(standard.New(addr))
}
