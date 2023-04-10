package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/observiq/tracing/db"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

var tracer = otel.Tracer("ordersAPI")

type server struct {
	httpServer *http.Server
	db         *redis.Client
}

func newServer(serverAddr, redisAddr string) (*server, error) {
	mux := http.NewServeMux()
	httpServer := &http.Server{
		Addr:    serverAddr,
		Handler: mux,
	}
	c := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	if _, err := c.Ping(context.Background()).Result(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &server{
		httpServer: httpServer,
	}, nil
}

func (s *server) start() error {
	return s.httpServer.ListenAndServe()
}

func (s *server) stop() error {
	if err := s.db.Close(); err != nil {
		return errors.Join(fmt.Errorf("close redis: %w", err), s.httpServer.Close())
	}
	return s.httpServer.Close()
}

func initTraceProvider(ctx context.Context) (*trace.TracerProvider, error) {
	hostname, _ := os.Hostname()
	resources := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("ourservice"),
		semconv.HostArchKey.String(runtime.GOARCH),
		semconv.HostNameKey.String(hostname),
	)
	conn, err := grpc.DialContext(ctx, "localhost:4317", grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure(), otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, err
	}

	return trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(resources),
	), nil
}

func newRouter() (*gin.Engine, *gin.RouterGroup) {
	r := gin.New()
	v1 := r.Group("/v1")
	v1.Use(otelgin.Middleware("ordersAPI"))
	return r, v1
}

// Record an error on the span and abort the request with the given status code and error
func handleErrorResponse(c *gin.Context, span oteltrace.Span, statusCode int, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	c.AbortWithError(statusCode, err)
}

func getOrder(c *gin.Context, rc *db.Client) {
	ctx, span := tracer.Start(c.Request.Context(), "/order/:id")
	defer span.End()

	id := c.Param("id")
	if id == "" {
		err := errors.New("id is empty")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	span.SetAttributes(attribute.String("order.id", id))

	order, err := rc.Get(ctx, id)
	if err != nil && !errors.Is(err, redis.Nil) {
		handleErrorResponse(c, span, http.StatusInternalServerError, err)
		return
	}

	if order == "" || errors.Is(err, redis.Nil) {
		err := errors.New("order not found")
		handleErrorResponse(c, span, http.StatusNotFound, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"order": order,
	})
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	traceProvider, err := initTraceProvider(ctx)
	if err != nil {
		log.Fatal(err)
	}
	otel.SetTracerProvider(traceProvider)
	defer traceProvider.Shutdown(context.Background())

	c, err := db.NewClient(ctx, "localhost:6379")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	router, v1 := newRouter()
	v1.GET("/orders/:id", func(ctx *gin.Context) { getOrder(ctx, c) })

	s := &http.Server{
		Addr:    ":9911",
		Handler: router,
	}

	go func() {
		if err := s.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()
	<-ctx.Done()
	s.Shutdown(context.Background())
}
