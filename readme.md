go get github.com/gorilla/mux
go get github.com/streadway/amqp
go get github.com/spf13/viper
go get github.com/lib/pq

go mod tidy
sudo apt-get install libpq-dev
go get github.com/go-redis/redis/v8