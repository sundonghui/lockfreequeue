package lockfreequeue

import (
	"sync"
	"testing"
	"time"
)

var (
	standardBufferSize = uint32(256)
)

func TestConcurrentGetsAreSequentiallyOrdered(t *testing.T) {

	var buffer, _ = NewRingBuffer[int](standardBufferSize, 10)

	var wg sync.WaitGroup
	messages := []int{}

	for i := 0; i < 100000; i++ {
		messages = append(messages, i)
	}

	consumer, _ := buffer.CreateConsumer()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, value := range messages {
			buffer.Write(value)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, value := range messages {
			j := consumer.Read()
			if j != value {
				t.Fail()
			}
		}
	}()
	wg.Wait()
}

func TestConcurrentAddRemoveConsumerDoesNotBlockWrites(t *testing.T) {
	var (
		buffer, _ = NewRingBuffer[int](standardBufferSize, 10)
		wg        sync.WaitGroup
		cnt       = 10000
		messages  = make([]int, 0, cnt)
	)

	for i := 0; i < cnt; i++ {
		messages = append(messages, i)
	}

	consumer1, _ := buffer.CreateConsumer()
	failIfDeadLock(t)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, value := range messages {
			buffer.Write(value)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, value := range messages {
			j := consumer1.Read()
			if j != value {
				t.Fail()
			}
		}
	}()

	wg.Add(1)
	go func() {
		consumer2, _ := buffer.CreateConsumer()
		defer wg.Done()
		for range messages[:500] {
			consumer2.Read()
		}

		consumer2.Remove()
	}()

	wg.Wait()
}

func failIfDeadLock(t *testing.T) {
	// fail if routine is blocking
	go time.AfterFunc(1*time.Second, func() {
		t.FailNow()
	})
}
