package dispatcher

type RecordError struct {
	Index int
	Error error
}

type Processor[T any] interface {
	Process([]T) ([]RecordError, error)
}
