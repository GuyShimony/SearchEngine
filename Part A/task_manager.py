from multiprocessing import Pool, Process, cpu_count
from queue import Queue
from subprocess import check_output, PIPE, Popen


class taskManager:

    def __init__(self):
        self.__tasks = Queue()
        self.__pool = Pool(cpu_count())
        self.__processes = []
        self.wait = True

    def get_number_of_waiting_tasks(self):
        return self.__tasks.qsize()

    def add_task(self, task: tuple):
        """
        The function will add the new task to the tasks queue to wait for it turn
        param: task: Tuple. Should contain the function to execute, args of the function, if the task is related to
        accessing a file a third parameter should be passed - 'rf' for reading file, 'wb' for writing to a file. In 
        addition, the args should contain the path by using the syntax filename='path'
        example: (function_to_execute, args,)
                (function_to_execute, args - containing filename=path, 'wb')
        """
        self.__tasks.put(task)

    def stop(self):
        [p.join(self) for p in self.__processes]
        self.__pool.close()
        # self.__pool.join()
        self.wait = False

    def start(self):
        # Process(target=self.start_op).start()
        self.start_op()

    def start_op(self):
        while self.wait:
            if not self.__tasks.empty():
                task = self.__tasks.get()
                if len(task) == 3 and task[3] == 'wb':
                    try:
                        lsout = Popen(['lsof', task[1]['filename']], stdout=PIPE, shell=False)
                        check_output(["grep", task[1]['filename']], stdin=lsout.stdout, shell=False)
                    except:
                        self.__processes.append(Process(target=task[0], args=task[1]).start())
                
                else:
                    self.__processes.append(Process(target=task[0], args=task[1]).start())
                    # self.__pool.map(task[0], task[1])
