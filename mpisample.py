from mpi4py import MPI
import sys

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

TAG_SOLVE = 101
TAG_SOLVED = 102
TAG_QUIT = 103


status = MPI.Status()

class Job:
    def __init__(self, id, data):
        self.id = id
        self.data = data
        self.result = None

    def compute(self):
        # Write your own computation here
        self.result = self.data * 2

    def setresult(self, result):
        self.result = result

        
class JobList:
    def __init__(self):
        self.jobs = {}

    def add_data(self, id, data):
        self.jobs[id] = Job(id, data)

    def update(self, idx, result):
        self.jobs[idx].setresult(result)
        
class Master:
    def __init__(self, joblist, comm):
        self.joblist = joblist
        self.queue = joblist.jobs.keys()
        self.jobcount = len(self.queue)
        self.comm = comm

        self.solved = []
        self.finished = False
        
    def start(self, numworkers):
        # first stage, send one job to all workers
        numparts = self.jobcount
        if numparts > numworkers: # there is more work than workers
            num = numworkers
        else: # job count is less than workers
            num = numparts
            
        for i in range(1, num):
            job_id = self.queue.pop()            
            self.comm.send(self.joblist.jobs[job_id], dest=i, tag=TAG_SOLVE)
                   
        while not self.finished:
            print "remaining ", self.jobcount  - len(self.solved)
            msg = self.comm.recv(status=status) # dict { idx:solved_idx, result:result }
            tag = status.tag
            sender = status.Get_source()
            if tag == TAG_SOLVED:
                self.update(msg)
                if self.finished is False:
                    if len(self.queue) > 0:
                        nextjob = self.queue.pop()
                        self.comm.send(self.joblist.jobs[nextjob], dest=sender, tag= TAG_SOLVE )
                else:
                    break

        print "Finished"
        self.finalize()
        
    def finalize(self):
        for i in range(1, self.comm.Get_size()):
            self.comm.send(0, dest=i, tag=TAG_QUIT)
        sys.exit()
        
        
    def update(self, msg):
        self.joblist.update(msg["idx"], msg["result"])
        self.solved.append(msg["idx"])
        if (len(self.solved) == self.jobcount):
            self.finished = True
            
if rank == 0:
    print "Master"
    #load the data at master
    jobs = JobList()
    for i in range(10000):
        jobs.add_data(i,i)
        
    master = Master(jobs, comm)
    master.start(size)
    
else:
    while 1:
        data = comm.recv(source=0, status = status)
        tag = status.tag

        if tag == TAG_QUIT:
            sys.exit()

        if tag == TAG_SOLVE:
            # the data sent from the master is the job class 
            data.compute()
            comm.send({"idx":data.id, "result":data.result}, dest=0, tag=TAG_SOLVED)

