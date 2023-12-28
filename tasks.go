package parallel_priority_tasks

type TaskManager interface {
	/*
		PushTask will block if pending tasks are greater than the manager's maxPendingTasks, otherwise it is async.
		args can be nil. f should be nil if using a Manager with a preset F.

		priority:
			Execution order is priority-ascending: a task with 3 priority is executed after one with 0 priority.

			If given prio is lower than 0 or greater than the manager's maxPriority, it will be clamped. So a task with
			-1 priority will have the highest priority.

			Tasks with same priority will be executed on a fifo basis.
	*/
	PushTask(priority int, args interface{})
}

type taskManager struct {
	newTaskNotify chan bool
	queues        []chan interface{}
	maxPrio       int
	taskF         func(interface{})
}

func NewTaskManager(numWorkers int, maxPriority int, maxPendingTasks int, taskF func(interface{})) TaskManager {
	//sanity checks
	if numWorkers < 1 {
		numWorkers = 1
	}
	if maxPriority < 1 {
		maxPriority = 1
	}
	if maxPendingTasks < 1 {
		maxPendingTasks = 1
	}
	if taskF == nil {
		taskF = func(i interface{}) {
		}
	}

	newTaskNotify := make(chan bool, maxPendingTasks)
	queues := make([]chan interface{}, 0)
	for i := 0; i < maxPriority+1; i++ {
		queue := make(chan interface{}, maxPendingTasks)
		queues = append(queues, queue)
	}
	t := taskManager{
		newTaskNotify: newTaskNotify,
		queues:        queues,
		maxPrio:       maxPriority,
		taskF:         taskF,
	}
	t.initWorkers(numWorkers)
	return &t
}

func (t *taskManager) initWorkers(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		go func() {
			for {
				<-t.newTaskNotify
			TaskSearcher:
				for _, queue := range t.queues {
					select {
					case args := <-queue:
						t.taskF(args)
						break TaskSearcher
					default:
						//keep looking for tasks down lower prio queues
					}
				}
			}
		}()
	}
}

func (t *taskManager) PushTask(priority int, args interface{}) {
	//clamp priority
	if priority < 0 {
		priority = 0
	}
	if priority > t.maxPrio {
		priority = t.maxPrio
	}
	//queue task
	t.queues[priority] <- args
	//notify workers of new task
	t.newTaskNotify <- true
}
