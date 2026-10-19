# CodeRisk 业务后端在容器内编译和测试，不依赖宿主机 Go。
# 本镜像只服务当前切片的构建与测试，不包含 Agent、前端或 Kafka。
FROM golang:1.24.6

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

CMD ["go", "test", "-count=1", "./..."]
