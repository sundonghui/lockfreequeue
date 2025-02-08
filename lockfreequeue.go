package lockfreequeue

import (
	"errors"
	"runtime"
	"sync/atomic"
)

var (
	InvalidBufferSize       = errors.New("buffer must be of size 2^n")
	NoAvaliableConsumerSlot = errors.New("no available consumer slot")
)

type RingBuffer[T any] struct {
	size              uint32   // 缓冲区大小
	bitWiseValue      uint32   // 位运算值，用于快速计算索引
	headIndex         uint32   // 当前写入的位置
	nextReaderIndex   uint32   // 下一个消费者的索引
	buffer            []T      // 存储数据的缓冲区
	readerIndexes     []uint32 // 每个消费者读取数据的位置
	readerActiveFlags []uint32 // 每个消费者的状态标志
}

type Consumer[T any] struct {
	ring *RingBuffer[T]
	id   uint32
}

func NewRingBuffer[T any](size uint32, readers uint32) (RingBuffer[T], error) {
	// 利用位运算来快速计算索引
	if size&(size-1) != 0 {
		return RingBuffer[T]{}, InvalidBufferSize
	}
	return RingBuffer[T]{
		size:              size,
		bitWiseValue:      size - 1,
		headIndex:         0,
		nextReaderIndex:   0,
		buffer:            make([]T, size, size),
		readerIndexes:     make([]uint32, readers),
		readerActiveFlags: make([]uint32, readers),
	}, nil
}

func (buffer *RingBuffer[T]) CreateConsumer() (Consumer[T], error) {
	for readerIndex := range buffer.readerActiveFlags {
		if atomic.CompareAndSwapUint32(&buffer.readerActiveFlags[readerIndex], 0, 2) {
			// 这里必须使用 atomic.LoadUint32，因为在并发情况下，headIndex 可能会被其他消费者修改
			buffer.readerIndexes[readerIndex] = atomic.LoadUint32(&buffer.headIndex)
			atomic.StoreUint32(&buffer.readerActiveFlags[readerIndex], 1)

			// 原子递增
			uRedaderIndex := uint32(readerIndex)
			atomic.CompareAndSwapUint32(&buffer.nextReaderIndex, uRedaderIndex, uRedaderIndex+1)

			return Consumer[T]{ring: buffer, id: uRedaderIndex}, nil
		}
	}
	// 没有可用的消费者位置
	return Consumer[T]{}, NoAvaliableConsumerSlot
}

func (buffer *RingBuffer[T]) Write(value T) {
	/*
		Tips:
		uint32 溢出后会从 0 重新开始，不会崩溃。
		这种特性特别适合环形缓冲区，可以让索引自然循环，不会超出范围。
	*/
	var (
		offset uint32
		i      uint32
	)

attemptWrite:
	nextReaderIndex := atomic.LoadUint32(&buffer.nextReaderIndex)

	for i = 0; i < nextReaderIndex; i++ {
		// 判断消费者是否正在读取数据
		if atomic.LoadUint32(&buffer.readerActiveFlags[i]) == 1 {
			offset = atomic.LoadUint32(&buffer.readerIndexes[i]) + buffer.size

			// 消费者读取的位置和当前写入位置重合，说明消费者已经读取了所有数据
			if offset == buffer.headIndex {
				runtime.Gosched() // 让出 CPU 时间片
				goto attemptWrite
			}
		}
	}

	// 写入数据
	// 不能使用 atomic.AddUint32(&buffer.headIndex, 1)
	// 测试发现多个生产者同时执行时，可能会导致连续增长
	// 某个生产者写入的数据可能会被覆盖
	nextIndex := buffer.headIndex + 1
	buffer.buffer[nextIndex&buffer.bitWiseValue] = value
	atomic.StoreUint32(&buffer.headIndex, nextIndex)
	return
}

func (consumer *Consumer[T]) Read() T {
	return consumer.ring.read(consumer.id)
}

func (consumer *Consumer[T]) Remove() {
	consumer.ring.removeConsumer(consumer.id)
}

func (buffer *RingBuffer[T]) read(readerIndex uint32) T {
	newIndex := buffer.readerIndexes[readerIndex] + 1

	// 等待写入数据
	for newIndex > atomic.LoadUint32(&buffer.headIndex) {
		runtime.Gosched() // 让出 CPU 时间片
	}

	value := buffer.buffer[newIndex&buffer.bitWiseValue]
	atomic.AddUint32(&buffer.readerIndexes[readerIndex], 1)
	return value
}

func (buffer *RingBuffer[T]) removeConsumer(readerID uint32) {
	atomic.StoreUint32(&buffer.readerActiveFlags[readerID], 0)
	atomic.CompareAndSwapUint32(&buffer.nextReaderIndex, readerID-1, buffer.nextReaderIndex-1)
}
