package receiver

type Processor[T any] interface {
	Incoming() chan T
	Close()
	WaitForStop()
}
