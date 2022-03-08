import yaml
import time
from datetime import datetime
import threading

f = open("Milestone1B_Log.txt", "w")

task_dict = {}
with open('Milestone1B.yaml') as file:
    tasks = yaml.safe_load_all(file)
    
    for task in tasks:
        task_dict.update(task)


f.write(f"{datetime.now()};M1B_Workflow Entry \n")
main_f = task_dict['M1B_Workflow']

def TimeFunction(inpts,path,task):
    f.write(f"{datetime.now()};{path} Executing TimeFunction ({inpts['FunctionInput']},{inpts['ExecutionTime']}) \n")
    print("executing ......")    
    time.sleep(int(inpts['ExecutionTime']))
    print("done...")
    
    if(len(task) == 0):
        f.write(f"{datetime.now()};{path} Exit \n")
    else:
        f.write(f"{datetime.now()};{path}.{task} Exit \n")

def Activity(act,task,path):
    
    f.write(f"{datetime.now()};{path}.{task} Entry \n")
    if(act[task]['Type'] == 'Task'):
        fun = eval(act[task]['Function'])
        fun(act[task]['Inputs'],path+"."+task,"")
    elif act[task]['Type'] == 'Flow':
        if act[task]['Execution'] == 'Sequential':
                Seq(act[task],path+"."+task)
        elif act[task]['Execution'] == 'Concurrent':
                Conc(act[task],path+"."+task)
        f.write(f"{datetime.now()};{path}.{task} Exit \n")

def Conc(main_f,path):
    act = main_f['Activities']
    
    threads = []
    
    for task in act:
        t = threading.Thread(target=Activity,args=(act,task,path))
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()

def Seq(main_f,path):
    act = main_f['Activities']

    for task in act:
        
        f.write(f"{datetime.now()};{path}.{task} Entry \n")

        if act[task]['Type'] == 'Task':
            fun = eval(act[task]['Function'])
            fun(act[task]['Inputs'],path+"."+task,"")
        elif act[task]['Type'] == 'Flow':
            if act[task]['Execution'] == 'Sequential':
                Seq(act[task],path+"."+task)
            elif act[task]['Execution'] == 'Concurrent':
                Conc(act[task],path+"."+task)
            f.write(f"{datetime.now()};{path}.{task} Exit \n")

if main_f['Execution'] == 'Sequential':
    Seq(main_f,"M1B_Workflow")
    f.write(f"{datetime.now()};M1B_Workflow Exit\n")