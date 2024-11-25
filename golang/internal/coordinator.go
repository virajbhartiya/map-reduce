package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mTasks     map[string]*TaskMeta
	rTasks     map[string]*TaskMeta
	cond       *sync.Cond
	mRemaining int
	rRemaining int
	nReduce    int
}

type TaskMeta struct {
	number    int
	startTime time.Time
	status    Status
}

type Status string

var (
	unstarted  Status = "unstarted"
	inprogress Status = "inprogress"
	completed  Status = "completed"
)

func (c *Coordinator) getMTask() (string, int) {
	for task := range c.mTasks {
		if c.mTasks[task].status == unstarted {
			c.mTasks[task].startTime = time.Now().UTC()
			c.mTasks[task].status = inprogress
			return task, c.mTasks[task].number
		}
	}
	return "", 0

}

func (c *Coordinator) getRTask() (string, int) {
	for task := range c.rTasks {
		if c.rTasks[task].status == unstarted {
			c.rTasks[task].startTime = time.Now().UTC()
			c.rTasks[task].status = inprogress
			return task, c.rTasks[task].number
		}
	}
	return "", 0

}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	c.cond.L.Lock()
	if c.mRemaining != 0 {
		mTask, mNumber := c.getMTask()
		for mTask == "" {
			if c.mRemaining == 0 {
				break
			}
			c.cond.Wait()
			mTask, mNumber = c.getMTask()
		}
		if mTask != "" {
			reply.Name = mTask
			reply.Number = mNumber
			reply.Type = mType
			reply.NReduce = c.nReduce
			c.cond.L.Unlock()
			return nil
		}
	}

	if c.rRemaining != 0 {
		rTask, rNumber := c.getRTask()
		for rTask == "" {
			if c.rRemaining == 0 {
				c.cond.L.Unlock()
				return errors.New("task completed, no more tasks")
			}
			c.cond.Wait()
			rTask, rNumber = c.getRTask()
		}
		reply.Name = rTask
		reply.Number = rNumber
		reply.Type = rType
		c.cond.L.Unlock()
		return nil
	}

	c.cond.L.Unlock()
	return errors.New("task completed, no more tasks")
}

func (c *Coordinator) UpdateTaskStatus(args *UpdateTaskStatusArgs, reply *UpdateTaskStatusReply) error {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	if args.Type == mType {
		c.mTasks[args.Name].status = completed
		c.mRemaining -= 1
	} else {
		c.rTasks[args.Name].status = completed
		c.rRemaining -= 1
	}
	return nil
}

func (c *Coordinator) rescheduler() {
	for {
		c.cond.L.Lock()
		if c.mRemaining != 0 {
			for task := range c.mTasks {
				currTime := time.Now().UTC()
				startTime := c.mTasks[task].startTime
				status := c.mTasks[task].status
				if status == inprogress {
					diff := currTime.Sub(startTime).Seconds()
					if diff > 10 {
						log.Printf("rescheduling task with name '%s', type '%s'", task, mType)
						c.mTasks[task].status = unstarted
						c.cond.Broadcast()
					}
				}
			}
		} else if c.rRemaining != 0 {
			c.cond.Broadcast()
			for task := range c.rTasks {
				currTime := time.Now().UTC()
				startTime := c.rTasks[task].startTime
				status := c.rTasks[task].status
				if status == inprogress {
					diff := currTime.Sub(startTime).Seconds()
					if diff > 10 {
						log.Printf("rescheduling task with name '%s', type '%s'", task, rType)
						c.rTasks[task].status = unstarted
						c.cond.Broadcast()
					}
				}
			}
		} else {
			c.cond.Broadcast()
			c.cond.L.Unlock()
			break
		}
		c.cond.L.Unlock()
	}
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	ret := false
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	if c.rRemaining == 0 {
		ret = true
	}
	return ret
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mTasks := map[string]*TaskMeta{}
	for i, file := range files {
		mTasks[file] = &TaskMeta{
			number: i,
			status: unstarted,
		}
	}
	rTasks := map[string]*TaskMeta{}
	for i := 0; i < nReduce; i++ {
		rTasks[fmt.Sprintf("%d", i)] = &TaskMeta{
			number: i,
			status: unstarted,
		}
	}
	mu := sync.Mutex{}
	cond := sync.NewCond(&mu)
	c := Coordinator{
		mTasks:     mTasks,
		rTasks:     rTasks,
		mRemaining: len(files),
		rRemaining: nReduce,
		nReduce:    nReduce,
		cond:       cond,
	}

	go c.rescheduler()

	c.server()
	return &c
}
