package utils

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

type ErrorResponse struct {
	Code    int
	Message string
}

// HandleError 错误处理(响应错误)
func HandleError(g *gin.Context, err error) {

	g.JSON(
		http.StatusOK, ErrorResponse{
			Code:    -1,
			Message: err.Error(),
		})
	g.Abort()
	return

}
