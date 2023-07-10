# Use the official Golang image as the base image
FROM golang:1.18

# Set the working directory inside the container
WORKDIR /app

# Copy the go.mod and go.sum files to the working directory
COPY go.mod go.sum ./

# Download the Go module dependencies
RUN go mod download

# Copy the rest of the project files to the working directory
COPY . .

# Build the Go application
RUN go build -o main .

# Set the entry point command to run the application
CMD ["./main"]

