package ginx

import (
	"github.com/gin-gonic/gin"

	"github.com/bailalayoub/rebound/httpx"
)

// Stream returns a Gin handler that serves the stream whose id is the path parameter param, for
// example engine.GET("/streams/:id", ginx.Stream(handler, "id")).
func Stream(x *httpx.Handler, param string) gin.HandlerFunc {
	return func(c *gin.Context) {
		x.ServeStream(c.Writer, c.Request, c.Param(param))
	}
}

// Cancel returns a Gin handler that cancels the stream whose id is the path parameter param, for
// example engine.POST("/streams/:id/cancel", ginx.Cancel(handler, "id")).
func Cancel(x *httpx.Handler, param string) gin.HandlerFunc {
	return func(c *gin.Context) {
		x.ServeCancel(c.Writer, c.Request, c.Param(param))
	}
}
