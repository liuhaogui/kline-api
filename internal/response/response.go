package response

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// AppLoggerContextKey is the gin context key for *logrus.Logger (set by middleware.AppLogger).
const AppLoggerContextKey = "app_logger"

func logBizCode(c *gin.Context, bizCode int) {
	if bizCode == 0 {
		return
	}
	raw, ok := c.Get(AppLoggerContextKey)
	if !ok {
		return
	}
	lg, ok := raw.(*logrus.Logger)
	if !ok || lg == nil {
		return
	}
	lg.WithFields(logrus.Fields{
		"event":      "api_response_error",
		"request_id": c.GetString("request_id"),
		"method":     c.Request.Method,
		"path":       c.Request.URL.Path,
		"route":      c.FullPath(),
		"code":       bizCode,
	}).Warn("api response business code not 0")
}

func withBizCode(bizCode int, payload any) any {
	if payload == nil {
		return gin.H{"code": bizCode}
	}
	switch v := payload.(type) {
	case gin.H:
		out := make(gin.H, len(v)+1)
		for k, val := range v {
			out[k] = val
		}
		if _, exists := out["code"]; !exists {
			out["code"] = bizCode
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v)+1)
		for k, val := range v {
			out[k] = val
		}
		if _, exists := out["code"]; !exists {
			out["code"] = bizCode
		}
		return out
	default:
		data, err := json.Marshal(payload)
		if err != nil {
			return gin.H{"code": bizCode, "data": payload}
		}
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil || m == nil {
			return gin.H{"code": bizCode, "data": payload}
		}
		if _, exists := m["code"]; !exists {
			m["code"] = bizCode
		}
		return m
	}
}

// API responds with HTTP 200; business status is in JSON field "code" (0 = success).
func API(c *gin.Context, bizCode int, payload any) {
	logBizCode(c, bizCode)
	c.JSON(http.StatusOK, withBizCode(bizCode, payload))
}

// AbortAPI is like API but aborts the handler chain (middleware).
func AbortAPI(c *gin.Context, bizCode int, payload any) {
	logBizCode(c, bizCode)
	c.AbortWithStatusJSON(http.StatusOK, withBizCode(bizCode, payload))
}

// JSON writes JSON with the given HTTP status (legacy; prefer API for JSON APIs).
func JSON(c *gin.Context, httpStatus int, obj any) {
	if httpStatus != http.StatusOK {
		logBizCode(c, httpStatus)
	}
	c.JSON(httpStatus, obj)
}

// AbortJSON wraps gin.Context.AbortWithStatusJSON (legacy; prefer AbortAPI).
func AbortJSON(c *gin.Context, httpStatus int, obj any) {
	if httpStatus != http.StatusOK {
		logBizCode(c, httpStatus)
	}
	c.AbortWithStatusJSON(httpStatus, obj)
}
