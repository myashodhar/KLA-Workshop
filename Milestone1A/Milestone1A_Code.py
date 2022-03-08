import time
import yaml
from datetime import datetime

task_dict = {}

f = open("Milestone1A_Log.txt", 'w')

with open('Milestone1A.yaml', 'r') as file:
    tasks = yaml.safe_load_all(file)
    
    for task in tasks:
        task_dict.update(task)

f.write(f"{datetime.now()};M1A_Workflow Entry \n")
main_f = task_dict['M1A_Workflow']

def TimeFunction(inpts,path):
    f.write(f"{datetime.now()};{path} Executing TimeFunction ({inpts['FunctionInput']},{inpts['ExecutionTime']}) \n")
    print("executing ......")
    time.sleep(int(inpts['ExecutionTime']))
    print("done...")
    
def Seq(main_f,path):
    act = main_f['Activities']

    for task in act:
        
        f.write(f"{datetime.now()};{path}.{task} Entry \n")

        if act[task]['Type'] == 'Task':
            fun = eval(act[task]['Function'])
            fun(act[task]['Inputs'],path+"."+task)
        elif act[task]['Type'] == 'Flow':
            if act[task]['Execution'] == 'Sequential':
                Seq(act[task],path+"."+task)
                
        f.write(f"{datetime.now()};{path}.{task} Exit \n")


if main_f['Execution'] == 'Sequential':
    Seq(main_f,"M1A_Workflow")
    f.write(f"{datetime.now()};M1A_Workflow Exit\n")
