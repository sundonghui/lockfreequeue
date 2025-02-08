package lockfreequeue

import (
	"sync"
	"testing"
)

var (
	MessageCount = 5000000
)

func BenchmarkConsumerSequentialReadWrite(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ConsumerSequentialReadWrite(MessageCount, b) //  1.789s
	}
}

func BenchmarkChannelsSequentialReadWriteLarge(b *testing.B) {
	for n := 0; n < b.N; n++ {
		ChannelsSequentialReadWrite(MessageCount, b) // 2.861s
	}
}

func ConsumerSequentialReadWrite(n int, b *testing.B) {

	b.StopTimer()
	var buffer, _ = NewRingBuffer[int](standardBufferSize, 10)
	consumer, _ := buffer.CreateConsumer()
	b.StartTimer()

	for i := 0; i < n; i++ {
		buffer.Write(i)
		consumer.Read()
	}
}

func ChannelsSequentialReadWrite(n int, b *testing.B) {

	b.StopTimer()
	var buffer = make(chan int, standardBufferSize)
	b.StartTimer()

	for i := 0; i < n; i++ {
		buffer <- i
		<-buffer
	}
}

func BenchmarkConsumerConcurrentReadWrite(b *testing.B) {
	for n := 0; n < b.N; n++ {
		ConsumerConcurrentReadWrite(MessageCount, b) // 3.320s
	}
}

func BenchmarkChannelsConcurrentReadWrite(b *testing.B) {
	for n := 0; n < b.N; n++ {
		ChannelsConcurrentReadWrite(MessageCount, b) // 3.003s
	}
}

func ConsumerConcurrentReadWrite(n int, b *testing.B) {

	b.StopTimer()
	var buffer, _ = NewRingBuffer[int](standardBufferSize, 10)

	var wg sync.WaitGroup
	messages := []int{}

	for i := 0; i < n; i++ {
		messages = append(messages, i)
	}

	consumer, _ := buffer.CreateConsumer()
	b.StartTimer()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, value := range messages {
			buffer.Write(value)
		}
	}()

	wg.Add(1)
	go func() {
		i := -1
		defer wg.Done()
		for _, _ = range messages {
			j := consumer.Read()
			if j != i+1 {
				panic("data is inconsistent")
			}
			i = j
		}
	}()
	wg.Wait()
}

func ChannelsConcurrentReadWrite(n int, b *testing.B) {

	b.StopTimer()
	var wg sync.WaitGroup
	messages := []int{}

	for i := 0; i < n; i++ {
		messages = append(messages, i)
	}

	var buffer = make(chan int, standardBufferSize)
	b.StartTimer()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, value := range messages {
			buffer <- value
		}
	}()

	wg.Add(1)
	go func() {
		i := -1
		defer wg.Done()
		for _, _ = range messages {
			j := <-buffer
			if j != i+1 {
				panic("data is inconsistent")
			}
			i = j
		}
	}()
	wg.Wait()
}
