from threading import Thread, Event
from typing import Iterator, Any, Callable, Optional, Union, Tuple
from queue import Queue


def set_event(ev: Event, flag: bool = True):
    if flag:
        ev.set()
    else:
        ev.clear()


class TerminateException(BaseException):
    pass


class DataFlowThread(Thread):
    queue: Queue
    input_timeout: Optional[float] = None
    lazy_start: bool = True
    _input_it: Optional[Iterator[Any]] = None
    _has_input: Event = Event()
    _terminate: bool = False

    def __init__(self,
                 main_loop: Callable, max_queue: int = 1, input_it: Optional[Iterator[Any]] = None,
                 lazy_start: bool = True,
                 *thread_args, **thread_kwargs):
        self.queue = Queue(maxsize=max_queue)
        self.set_input(input_it)
        self.lazy_start = lazy_start
        self.main_loop = main_loop

        Thread.__init__(self, target=None, daemon=True, *thread_args, **thread_kwargs)

    def run(self) -> None:
        self.main_loop(self)
        self.terminate()

    def __iter__(self):
        return self

    def __next__(self):
        if self._terminate:
            if self.queue.empty():
                raise StopIteration()
        elif not self.is_alive() and self.lazy_start:
            self.start()
        return self.pop(self.input_timeout)

    def set_input(self, input_it=None):
        self._input_it = input_it
        set_event(self._has_input, self._input_it is not None)

    def iter_input(self, timeout: Optional[float] = None, raise_exception: bool = False, break_on_end: bool = False):
        while not self._terminate:
            item, err = self.receive(timeout)
            if raise_exception and err:
                raise err
            if item is None and break_on_end:
                break
            yield item

    def pop(self, timeout: Optional[float] = None) -> Any:
        if self._terminate and self.queue.empty():
            return None
        else:
            result = self.queue.get(block=True, timeout=timeout)
            self.queue.task_done()
            return result

    def terminate(self):
        self._terminate = True

    # def set_pause_producing(self, pause: bool = True):
    #     self.queue.pause_push = pause
    #
    # def set_pause_sending(self, pause: bool = True):
    #     self.queue.pause_pop = pause

    def receive(self, timeout: Optional[float] = None) -> Tuple[Any, Optional[Exception]]:
        try:
            success = self._has_input.wait(timeout)
            if not success:
                return None, TimeoutError()
            return next(self._input_it), None
        except Exception as e:
            self.set_input(None)
            return None, e

    def send(self, item):
        if self._terminate:
            raise TerminateException()
        self.queue.put(item, block=True)


class GeneratorDataFlowThread(DataFlowThread):
    def __init__(self,
                 func: Callable[[DataFlowThread], Iterator], max_queue: int = 1, input_it: Optional[Iterator[Any]] = None,
                 lazy_start: bool = True,
                 *thread_args, **thread_kwargs):
        self.func = func
        self.queue = Queue(maxsize=max_queue)
        self.set_input(input_it)
        self.lazy_start = lazy_start
        Thread.__init__(self, daemon=True, *thread_args, **thread_kwargs)

    def run(self) -> None:
        for item in self.func(self):
            if self._terminate:
                break
            self.send(item)
        self.terminate()


class StepDataFlowThread(GeneratorDataFlowThread):
    def __init__(self,
                 step: Callable[[DataFlowThread], Any], max_queue: int = 1, input_it: Optional[Iterator[Any]] = None,
                 lazy_start: bool = True,
                 *thread_args, **thread_kwargs):
        def loop(this: StepDataFlowThread) -> Iterator[Any]:
            while not this._terminate:
                yield step(this)
        super().__init__(func=loop, max_queue=max_queue, input_it=input_it,
                         lazy_start=lazy_start,
                         *thread_args, **thread_kwargs)
