package fanout

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
)

type FanoutInMemoryDesign interface {
	gface.Module
	OffBus(name string) bool
	OnBus(name string) bool
}

type FanoutInMemory[T any] struct {
	inputBus *eventbus.EventBus[T]
	name     string
	busList  []*BusInfo[T]

	stop chan struct{}
	wg   sync.WaitGroup

	mu          sync.RWMutex
	closedFlag  atomic.Bool
	startedFlag atomic.Bool
}

var _ FanoutInMemoryDesign = (*FanoutInMemory[any])(nil)

type BusInfo[T any] struct {
	busName  string
	bus      *eventbus.EventBus[T]
	isClosed bool
}

type BusRequest struct {
	BusName   string
	WantClose bool
}

func NewFanoutInMemory[T any](name string, busRequest []BusRequest, inputCap, outputCap int) *FanoutInMemory[T] {
	fanountName := fmt.Sprintf("EventbusForFanoutFor%s", name)
	inputBus := eventbus.NewInMemory[T](inputCap, gface.ModuleName(fanountName))

	busList := make([]*BusInfo[T], len(busRequest))
	for i, e := range busRequest {
		newOutputBus := eventbus.NewInMemory[T](outputCap, gface.ModuleName(e.BusName))
		newBusInfo := &BusInfo[T]{
			busName:  e.BusName,
			bus:      newOutputBus,
			isClosed: e.WantClose,
		}
		busList[i] = newBusInfo
	}

	f := &FanoutInMemory[T]{
		inputBus: inputBus,
		name:     name,
		busList:  busList,
		stop:     make(chan struct{}),
		wg:       sync.WaitGroup{},
		mu:       sync.RWMutex{},
	}

	return f
}
func (f *FanoutInMemory[T]) Name() gface.ModuleName {
	return gface.ModuleName(f.name)
}
func (f *FanoutInMemory[T]) Start(ctx context.Context) error {
	defer fmt.Printf("%s fanout Start 중단함.\n", f.name)
	if f.startedFlag.Swap(true) {
		return fmt.Errorf("fanout %q already started", f.name)
	}

	f.wg.Add(1)
	defer f.wg.Done()

	input := f.inputBus.Dequeue()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-f.stop:
			return nil
		case msg, ok := <-input:
			if !ok {
				return nil
			}
			if err := f.fanoutMsg(ctx, msg); err != nil {
				return err
			}
		}
	}

}

func (f *FanoutInMemory[T]) fanoutMsg(ctx context.Context, msg T) error {
	f.mu.RLock()
	//캡쳐
	buses := make([]*BusInfo[T], 0, len(f.busList))
	for _, b := range f.busList {
		if !b.isClosed {
			buses = append(buses, b)
		}
	}
	f.mu.RUnlock()

	var lastError error
	for _, e := range buses {
		err := e.bus.Enqueue(ctx, msg)
		if err != nil {
			fmt.Printf("[FanoutInMemory] failed to publish to bus %s: %v\n", e.busName, err)
			lastError = err
		}
	}

	return lastError
}
func (f *FanoutInMemory[T]) Closed() bool {
	return f.closedFlag.Load()
}

func (f *FanoutInMemory[T]) Close() error {
	//이미 닫혔으면 즉시 반환
	if f.closedFlag.Swap(true) {
		return nil
	}
	close(f.stop)

	f.wg.Wait()

	f.inputBus.Close()
	f.mu.Lock()
	for _, b := range f.busList {
		b.bus.Close()
	}
	f.mu.Unlock()
	fmt.Printf("%s fandoutManager Closed\n", f.name)
	return nil
}

func (f *FanoutInMemory[T]) GetBus(name string) *eventbus.EventBus[T] {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, b := range f.busList {
		if b.busName == name {
			return b.bus
		}
	}
	return nil
}

func (f *FanoutInMemory[T]) OffBus(name string) (targetExist bool) {
	return f.switchTarget(name, true)
}
func (f *FanoutInMemory[T]) OnBus(name string) (targetExist bool) {
	return f.switchTarget(name, false)
}

func (f *FanoutInMemory[T]) switchTarget(name string, wantClose bool) (targetExist bool) {
	defer f.mu.Unlock()
	f.mu.Lock()
	targetExist = false
	for _, e := range f.busList {
		if e.busName == name {
			e.isClosed = wantClose
			targetExist = true
			return targetExist
		}
	}
	return false
}

func (f *FanoutInMemory[T]) Publish(ctx context.Context, msg T) error {
	return f.inputBus.Enqueue(ctx, msg)
}
