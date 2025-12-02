package dispatcher

type Processor[T any] interface {
	Process(T) error
}
